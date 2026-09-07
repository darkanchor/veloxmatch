#define _GNU_SOURCE
#include "ombus/om_bus_mp.h"
#include <inttypes.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Measurement only: no replacement queue or change to reservation/commit.
 * Build against HEAD with BENCH_HAS_COPY=0 for the identical legacy workload.
 * poll mode copies AFTER release (legacy, unsafe under reuse); copy mode uses
 * the proposed ownership-safe API. Report any payload/ordering failure.
 */
#ifndef BENCH_HAS_COPY
#define BENCH_HAS_COPY 1
#endif
#define BYTES 336U
#define WORDS (BYTES / sizeof(uint64_t))
static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}
static void pin(unsigned cpu) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (pthread_setaffinity_np(pthread_self(), sizeof(set), &set)) abort();
}
static void pause_cpu(void) { __asm__ volatile("pause"); }
typedef struct Producer {
    OmBusMpProducer bus;
    pthread_t thread;
    uint64_t count;
    uint64_t retries;
    unsigned id;
    _Atomic unsigned *start;
} Producer;
static void payload_set(uint64_t *payload, uint64_t index, unsigned id) {
    payload[0] = index;
    payload[1] = id;
    for (unsigned j = 2; j < WORDS; j++) payload[j] = index ^ ((uint64_t)id << 32) ^ j;
}
static void *produce(void *arg) {
    Producer *p = arg;
    const unsigned cpus[] = {0, 2, 4};
    pin(cpus[p->id]);
    while (!atomic_load_explicit(p->start, memory_order_acquire)) pause_cpu();
    uint64_t payload[WORDS];
    for (uint64_t i = 0; i < p->count; i++) {
        payload_set(payload, i, p->id);
        int rc;
        while ((rc = om_bus_mp_publish(&p->bus, payload, BYTES, NULL)) == OM_BUS_MP_ERR_FULL) {
            p->retries++;
            pause_cpu();
        }
        if (rc != 0) abort();
    }
    return NULL;
}
static int consume(OmBusMpConsumer *consumer, OmBusMpRecord *record,
                   uint64_t *owned, int copy) {
#if BENCH_HAS_COPY
    if (copy) return om_bus_mp_poll_copy(consumer, record, owned, BYTES);
#else
    if (copy) abort();
#endif
    int rc = om_bus_mp_poll(consumer, record);
    if (rc == OM_BUS_MP_POLL_RECORD) {
        if (record->payload_len != BYTES) abort();
        memcpy(owned, record->payload, BYTES);
    }
    return rc;
}
int main(int argc, char **argv) {
    if (argc != 4) {
        fprintf(stderr, "usage: %s poll|copy producers(0=serial,1,3) records_per_producer\n", argv[0]);
        return 2;
    }
    int copy = strcmp(argv[1], "copy") == 0;
    if (!copy && strcmp(argv[1], "poll")) return 2;
    unsigned producers = (unsigned)strtoul(argv[2], NULL, 10);
    uint64_t count = strtoull(argv[3], NULL, 10);
    if ((producers != 0 && producers != 1 && producers != 3) || !count) return 2;
    pin(6);
    OmBusMpConfig cfg = {.capacity=4096, .slot_size=512, .max_producers=3,
                        .skip_timeout_ns=10000000};
    void *memory = NULL;
    if (posix_memalign(&memory, 64, om_bus_mp_size(&cfg))) return 1;
    if (om_bus_mp_init(memory, om_bus_mp_size(&cfg), &cfg)) return 1;
    OmBusMpConsumer consumer;
    if (om_bus_mp_consumer_open(&consumer, memory)) return 1;
    _Atomic unsigned start = 0;
    Producer workers[3] = {0};
    for (unsigned i = 0; i < (producers ? producers : 1); i++) {
        workers[i].count = count;
        workers[i].id = i;
        workers[i].start = &start;
        if (om_bus_mp_producer_open(&workers[i].bus, memory, i)) return 1;
        if (producers && pthread_create(&workers[i].thread, NULL, produce, &workers[i])) return 1;
    }
    uint64_t total = count * (producers ? producers : 1);
    uint64_t errors = 0, next[3] = {0}, owned[WORDS], payload[WORDS];
    uint64_t t0 = now_ns();
    atomic_store_explicit(&start, 1, memory_order_release);
    for (uint64_t i = 0; i < total; i++) {
        if (!producers) {
            payload_set(payload, i, 0);
            if (om_bus_mp_publish(&workers[0].bus, payload, BYTES, NULL)) abort();
        }
        OmBusMpRecord record;
        int rc;
        while ((rc = consume(&consumer, &record, owned, copy)) == OM_BUS_MP_POLL_EMPTY) pause_cpu();
        if (rc != OM_BUS_MP_POLL_RECORD) {
            fprintf(stderr, "unexpected poll=%d at sequence=%" PRIu64 "\n", rc, i);
            return 1;
        }
        if (record.sequence != i || record.producer_id >= 3 || record.payload_len != BYTES) abort();
        uint64_t expected = next[record.producer_id]++;
        if (owned[0] != expected || owned[1] != record.producer_id) errors++;
        for (unsigned j = 2; j < WORDS; j++)
            if (owned[j] != (expected ^ ((uint64_t)record.producer_id << 32) ^ j)) errors++;
    }
    uint64_t elapsed = now_ns() - t0, retries = 0;
    for (unsigned i = 0; i < producers; i++) {
        pthread_join(workers[i].thread, NULL);
        retries += workers[i].retries;
        if (next[i] != count) abort();
    }
    OmBusMpStats stats;
    om_bus_mp_stats(memory, &stats);
    if (stats.dequeue_pos != total || stats.records_published != total || stats.skipped_sequences) abort();
    printf("{\"mode\":\"%s\",\"producers\":%u,\"payload_bytes\":%u,\"records\":%" PRIu64
           ",\"ns_per_record\":%.2f,\"records_per_second\":%.0f,\"full_retries\":%" PRIu64
           ",\"payload_errors\":%" PRIu64 ",\"skipped\":%" PRIu64 "}\n",
           argv[1], producers, BYTES, total, (double)elapsed/total,
           (double)total*1e9/elapsed, retries, errors, stats.skipped_sequences);
    free(memory);
    return errors ? 1 : 0;
}
