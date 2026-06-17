#include <check.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "ombus/om_bus_mp.h"

static void *bus_mp_alloc(const OmBusMpConfig *cfg, size_t *size_out) {
    size_t size = om_bus_mp_size(cfg);
    ck_assert_uint_gt(size, 0);
    void *memory = NULL;
    ck_assert_int_eq(posix_memalign(&memory, OM_BUS_MP_CACHELINE_SIZE, size), 0);
    ck_assert_ptr_nonnull(memory);
    memset(memory, 0, size);
    ck_assert_int_eq(om_bus_mp_init(memory, size, cfg), 0);
    if (size_out) *size_out = size;
    return memory;
}

START_TEST(test_bus_mp_init_invalid) {
    OmBusMpConfig cfg = {
        .capacity = 7,
        .slot_size = 64,
        .max_producers = 1,
    };
    ck_assert_uint_eq(om_bus_mp_size(&cfg), 0);

    uint8_t memory[1024];
    ck_assert_int_eq(om_bus_mp_init(memory, sizeof(memory), &cfg), OM_BUS_MP_ERR_NOT_POW2);

    cfg.capacity = 8;
    cfg.slot_size = 96;
    ck_assert_uint_eq(om_bus_mp_size(&cfg), 0);
    ck_assert_int_eq(om_bus_mp_init(memory, sizeof(memory), &cfg),
                     OM_BUS_MP_ERR_ALIGNMENT);
}
END_TEST

START_TEST(test_bus_mp_init_requires_aligned_memory) {
    OmBusMpConfig cfg = {
        .capacity = 8,
        .slot_size = 64,
        .max_producers = 1,
    };
    size_t size = om_bus_mp_size(&cfg);
    ck_assert_uint_gt(size, 0);

    void *raw = malloc(size + OM_BUS_MP_CACHELINE_SIZE);
    ck_assert_ptr_nonnull(raw);
    void *unaligned = (char *)raw + 1;
    ck_assert_int_eq(om_bus_mp_init(unaligned, size, &cfg), OM_BUS_MP_ERR_ALIGNMENT);
    free(raw);
}
END_TEST

START_TEST(test_bus_mp_single_publish_poll) {
    OmBusMpConfig cfg = {
        .capacity = 8,
        .slot_size = 64,
        .max_producers = 2,
    };
    void *memory = bus_mp_alloc(&cfg, NULL);

    OmBusMpProducer producer;
    OmBusMpConsumer consumer;
    ck_assert_int_eq(om_bus_mp_producer_open(&producer, memory, 1), 0);
    ck_assert_int_eq(om_bus_mp_consumer_open(&consumer, memory), 0);

    uint32_t payload = 0xAABBCCDDU;
    uint64_t sequence = UINT64_MAX;
    ck_assert_int_eq(om_bus_mp_publish(&producer, &payload, sizeof(payload), &sequence), 0);
    ck_assert_uint_eq(sequence, 0);

    OmBusMpRecord rec;
    ck_assert_int_eq(om_bus_mp_poll(&consumer, &rec), OM_BUS_MP_POLL_RECORD);
    ck_assert_uint_eq(rec.sequence, 0);
    ck_assert_uint_eq(rec.producer_id, 1);
    ck_assert_uint_eq(rec.payload_len, sizeof(payload));
    ck_assert_int_eq(memcmp(rec.payload, &payload, sizeof(payload)), 0);
    ck_assert_int_eq(om_bus_mp_poll(&consumer, &rec), OM_BUS_MP_POLL_EMPTY);

    free(memory);
}
END_TEST

START_TEST(test_bus_mp_full_fail_fast) {
    OmBusMpConfig cfg = {
        .capacity = 2,
        .slot_size = 64,
        .max_producers = 1,
    };
    void *memory = bus_mp_alloc(&cfg, NULL);

    OmBusMpProducer producer;
    OmBusMpConsumer consumer;
    ck_assert_int_eq(om_bus_mp_producer_open(&producer, memory, 0), 0);
    ck_assert_int_eq(om_bus_mp_consumer_open(&consumer, memory), 0);

    uint32_t payload = 1;
    ck_assert_int_eq(om_bus_mp_publish(&producer, &payload, sizeof(payload), NULL), 0);
    ck_assert_int_eq(om_bus_mp_publish(&producer, &payload, sizeof(payload), NULL), 0);
    ck_assert_int_eq(om_bus_mp_publish(&producer, &payload, sizeof(payload), NULL),
                     OM_BUS_MP_ERR_FULL);

    OmBusMpRecord rec;
    ck_assert_int_eq(om_bus_mp_poll(&consumer, &rec), OM_BUS_MP_POLL_RECORD);
    ck_assert_int_eq(om_bus_mp_publish(&producer, &payload, sizeof(payload), NULL), 0);

    OmBusMpStats stats;
    om_bus_mp_stats(memory, &stats);
    ck_assert_uint_eq(stats.full_rejects, 1);

    free(memory);
}
END_TEST

START_TEST(test_bus_mp_claim_commit) {
    OmBusMpConfig cfg = {
        .capacity = 4,
        .slot_size = 64,
        .max_producers = 1,
    };
    void *memory = bus_mp_alloc(&cfg, NULL);

    OmBusMpProducer producer;
    OmBusMpConsumer consumer;
    ck_assert_int_eq(om_bus_mp_producer_open(&producer, memory, 0), 0);
    ck_assert_int_eq(om_bus_mp_consumer_open(&consumer, memory), 0);

    OmBusMpClaim claim;
    ck_assert_int_eq(om_bus_mp_claim(&producer, sizeof(uint64_t), &claim), 0);
    ck_assert_uint_eq(claim.sequence, 0);
    uint64_t payload = 42;
    memcpy(claim.payload, &payload, sizeof(payload));
    ck_assert_int_eq(om_bus_mp_commit(&producer, &claim), 0);

    OmBusMpRecord rec;
    ck_assert_int_eq(om_bus_mp_poll(&consumer, &rec), OM_BUS_MP_POLL_RECORD);
    ck_assert_uint_eq(rec.sequence, 0);
    ck_assert_int_eq(memcmp(rec.payload, &payload, sizeof(payload)), 0);

    free(memory);
}
END_TEST

START_TEST(test_bus_mp_poison_unpublished_claim) {
    OmBusMpConfig cfg = {
        .capacity = 4,
        .slot_size = 64,
        .max_producers = 1,
        .skip_timeout_ns = 1000,
    };
    void *memory = bus_mp_alloc(&cfg, NULL);

    OmBusMpProducer producer;
    OmBusMpConsumer consumer;
    ck_assert_int_eq(om_bus_mp_producer_open(&producer, memory, 0), 0);
    ck_assert_int_eq(om_bus_mp_consumer_open(&consumer, memory), 0);

    OmBusMpClaim claim;
    ck_assert_int_eq(om_bus_mp_claim(&producer, sizeof(uint32_t), &claim), 0);

    OmBusMpRecord rec;
    ck_assert_int_eq(om_bus_mp_poll(&consumer, &rec), OM_BUS_MP_POLL_EMPTY);
    usleep(1000);
    ck_assert_int_eq(om_bus_mp_poll(&consumer, &rec), OM_BUS_MP_POLL_SKIPPED);
    ck_assert_uint_eq(rec.sequence, 0);

    uint32_t payload = 7;
    uint64_t sequence = UINT64_MAX;
    ck_assert_int_eq(om_bus_mp_publish(&producer, &payload, sizeof(payload), &sequence), 0);
    ck_assert_uint_eq(sequence, 1);
    ck_assert_int_eq(om_bus_mp_poll(&consumer, &rec), OM_BUS_MP_POLL_RECORD);
    ck_assert_uint_eq(rec.sequence, 1);
    ck_assert_int_eq(memcmp(rec.payload, &payload, sizeof(payload)), 0);

    ck_assert_int_eq(om_bus_mp_commit(&producer, &claim), OM_BUS_MP_ERR_SLOT_POISONED);

    OmBusMpStats stats;
    om_bus_mp_stats(memory, &stats);
    ck_assert_uint_eq(stats.skipped_sequences, 1);
    ck_assert_uint_eq(stats.poisoned_rejects, 1);

    free(memory);
}
END_TEST

typedef struct BusMpProducerThread {
    OmBusMpProducer producer;
    uint32_t count;
    uint32_t producer_id;
    int result;
} BusMpProducerThread;

static void *bus_mp_producer_thread(void *arg) {
    BusMpProducerThread *ctx = (BusMpProducerThread *)arg;
    for (uint32_t i = 0; i < ctx->count; i++) {
        uint64_t payload = ((uint64_t)ctx->producer_id << 32) | i;
        int rc = om_bus_mp_publish(&ctx->producer, &payload, sizeof(payload), NULL);
        if (rc != 0) {
            ctx->result = rc;
            return NULL;
        }
    }
    ctx->result = 0;
    return NULL;
}

START_TEST(test_bus_mp_concurrent_producers) {
    enum { producer_count = 4, per_producer = 64 };
    OmBusMpConfig cfg = {
        .capacity = 512,
        .slot_size = 64,
        .max_producers = producer_count,
    };
    void *memory = bus_mp_alloc(&cfg, NULL);

    pthread_t threads[producer_count];
    BusMpProducerThread ctx[producer_count];
    for (uint32_t i = 0; i < producer_count; i++) {
        ck_assert_int_eq(om_bus_mp_producer_open(&ctx[i].producer, memory, i), 0);
        ctx[i].count = per_producer;
        ctx[i].producer_id = i;
        ctx[i].result = -1;
        ck_assert_int_eq(pthread_create(&threads[i], NULL, bus_mp_producer_thread, &ctx[i]), 0);
    }

    for (uint32_t i = 0; i < producer_count; i++) {
        ck_assert_int_eq(pthread_join(threads[i], NULL), 0);
        ck_assert_int_eq(ctx[i].result, 0);
        ck_assert_uint_eq(om_bus_mp_producer_published(memory, i), per_producer);
    }

    OmBusMpConsumer consumer;
    ck_assert_int_eq(om_bus_mp_consumer_open(&consumer, memory), 0);

    OmBusMpRecord rec;
    for (uint64_t expected = 0; expected < producer_count * per_producer; expected++) {
        ck_assert_int_eq(om_bus_mp_poll(&consumer, &rec), OM_BUS_MP_POLL_RECORD);
        ck_assert_uint_eq(rec.sequence, expected);
        ck_assert_uint_lt(rec.producer_id, producer_count);
        ck_assert_uint_eq(rec.payload_len, sizeof(uint64_t));
    }
    ck_assert_int_eq(om_bus_mp_poll(&consumer, &rec), OM_BUS_MP_POLL_EMPTY);

    OmBusMpStats stats;
    om_bus_mp_stats(memory, &stats);
    ck_assert_uint_eq(stats.records_published, producer_count * per_producer);
    ck_assert_uint_eq(stats.dequeue_pos, producer_count * per_producer);

    free(memory);
}
END_TEST

START_TEST(test_bus_mp_poll_batch_drains_in_order) {
    OmBusMpConfig cfg = {
        .capacity = 16,
        .slot_size = 64,
        .max_producers = 1,
    };
    void *memory = bus_mp_alloc(&cfg, NULL);

    OmBusMpProducer producer;
    OmBusMpConsumer consumer;
    ck_assert_int_eq(om_bus_mp_producer_open(&producer, memory, 0), 0);
    ck_assert_int_eq(om_bus_mp_consumer_open(&consumer, memory), 0);

    /* Empty queue: batch returns 0, bad args return error. */
    OmBusMpRecord recs[8];
    ck_assert_int_eq(om_bus_mp_poll_batch(&consumer, recs, 8), 0);
    ck_assert_int_eq(om_bus_mp_poll_batch(NULL, recs, 8), OM_BUS_MP_ERR_INIT);
    ck_assert_int_eq(om_bus_mp_poll_batch(&consumer, NULL, 8), OM_BUS_MP_ERR_INIT);
    ck_assert_int_eq(om_bus_mp_poll_batch(&consumer, recs, 0), 0);

    const uint32_t total = 5;
    for (uint32_t i = 0; i < total; i++) {
        ck_assert_int_eq(om_bus_mp_publish(&producer, &i, sizeof(i), NULL), 0);
    }
    ck_assert_uint_eq(om_bus_mp_pending(&consumer), total);

    /* Bounded batch: ask for 3, get 3 in queue order. */
    int got = om_bus_mp_poll_batch(&consumer, recs, 3);
    ck_assert_int_eq(got, 3);
    for (uint32_t i = 0; i < 3; i++) {
        ck_assert_uint_eq(recs[i].sequence, i);
        ck_assert_uint_eq(recs[i].producer_id, 0);
        ck_assert_uint_eq(recs[i].payload_len, sizeof(uint32_t));
        ck_assert_int_eq(memcmp(recs[i].payload, &i, sizeof(uint32_t)), 0);
    }
    ck_assert_uint_eq(om_bus_mp_pending(&consumer), total - 3);

    /* Drain the rest: ask for more than remain, get exactly what's left. */
    got = om_bus_mp_poll_batch(&consumer, recs, 8);
    ck_assert_int_eq(got, 2);
    ck_assert_uint_eq(recs[0].sequence, 3);
    ck_assert_uint_eq(recs[1].sequence, 4);
    ck_assert_uint_eq(om_bus_mp_pending(&consumer), 0);
    ck_assert_int_eq(om_bus_mp_poll_batch(&consumer, recs, 8), 0);

    /* A single poll after a full drain still sees an empty queue (cursor state
     * stayed consistent across the batch path). */
    OmBusMpRecord one;
    ck_assert_int_eq(om_bus_mp_poll(&consumer, &one), OM_BUS_MP_POLL_EMPTY);

    free(memory);
}
END_TEST

START_TEST(test_bus_mp_poll_batch_stops_at_uncommitted_gap) {
    OmBusMpConfig cfg = {
        .capacity = 8,
        .slot_size = 64,
        .max_producers = 1,
        .skip_timeout_ns = 1000,
    };
    void *memory = bus_mp_alloc(&cfg, NULL);

    OmBusMpProducer producer;
    OmBusMpConsumer consumer;
    ck_assert_int_eq(om_bus_mp_producer_open(&producer, memory, 0), 0);
    ck_assert_int_eq(om_bus_mp_consumer_open(&consumer, memory), 0);

    /* Two committed records, then a claimed-but-uncommitted slot. */
    uint32_t a = 10, b = 20;
    ck_assert_int_eq(om_bus_mp_publish(&producer, &a, sizeof(a), NULL), 0);
    ck_assert_int_eq(om_bus_mp_publish(&producer, &b, sizeof(b), NULL), 0);
    OmBusMpClaim claim;
    ck_assert_int_eq(om_bus_mp_claim(&producer, sizeof(uint32_t), &claim), 0);

    /* Batch drains the two ready records and stops at the gap — it must NOT
     * perform skip-timeout recovery. */
    OmBusMpRecord recs[8];
    int got = om_bus_mp_poll_batch(&consumer, recs, 8);
    ck_assert_int_eq(got, 2);
    ck_assert_uint_eq(recs[0].sequence, 0);
    ck_assert_uint_eq(recs[1].sequence, 1);

    /* Still stuck at the gap until the single poll applies the skip timeout. */
    ck_assert_int_eq(om_bus_mp_poll_batch(&consumer, recs, 8), 0);
    OmBusMpRecord one;
    ck_assert_int_eq(om_bus_mp_poll(&consumer, &one), OM_BUS_MP_POLL_EMPTY);
    usleep(1000);
    ck_assert_int_eq(om_bus_mp_poll(&consumer, &one), OM_BUS_MP_POLL_SKIPPED);

    ck_assert_int_eq(om_bus_mp_commit(&producer, &claim), OM_BUS_MP_ERR_SLOT_POISONED);

    free(memory);
}
END_TEST

Suite *bus_mp_suite(void) {
    Suite *s = suite_create("BusMP");
    TCase *tc = tcase_create("MPSC");
    tcase_add_test(tc, test_bus_mp_init_invalid);
    tcase_add_test(tc, test_bus_mp_init_requires_aligned_memory);
    tcase_add_test(tc, test_bus_mp_single_publish_poll);
    tcase_add_test(tc, test_bus_mp_full_fail_fast);
    tcase_add_test(tc, test_bus_mp_claim_commit);
    tcase_add_test(tc, test_bus_mp_poison_unpublished_claim);
    tcase_add_test(tc, test_bus_mp_concurrent_producers);
    tcase_add_test(tc, test_bus_mp_poll_batch_drains_in_order);
    tcase_add_test(tc, test_bus_mp_poll_batch_stops_at_uncommitted_gap);
    suite_add_tcase(s, tc);
    return s;
}
