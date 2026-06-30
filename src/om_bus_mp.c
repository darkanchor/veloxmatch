#include "ombus/om_bus_mp.h"

#include <stdbool.h>
#include <errno.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#ifdef __linux__
#include <linux/futex.h>
#endif

typedef struct OmBusMpMetaLine {
    uint32_t magic;
    uint32_t version;
    uint32_t capacity;
    uint32_t slot_size;
    uint32_t max_producers;
    _Atomic uint32_t notify_seq;
    _Atomic uint32_t waiters;
    uint64_t skip_timeout_ns;
    uint8_t _pad[24];
} OmBusMpMetaLine;

typedef struct OmBusMpCursorLine {
    _Atomic uint64_t enqueue_pos;
    uint8_t _pad[56];
} OmBusMpCursorLine;

typedef struct OmBusMpConsumerCursorLine {
    _Atomic uint64_t dequeue_pos;
    uint8_t _pad[56];
} OmBusMpConsumerCursorLine;

typedef struct OmBusMpProducerStatsLine {
    _Atomic uint64_t records_published;
    _Atomic uint64_t full_rejects;
    _Atomic uint64_t poisoned_rejects;
    uint8_t _pad[40];
} OmBusMpProducerStatsLine;

typedef struct OmBusMpConsumerStatsLine {
    _Atomic uint64_t skipped_sequences;
    uint8_t _pad[56];
} OmBusMpConsumerStatsLine;

typedef struct OmBusMpHeader {
    OmBusMpMetaLine meta;
    OmBusMpCursorLine producer_cursor;
    OmBusMpConsumerCursorLine consumer_cursor;
    OmBusMpProducerStatsLine producer_stats;
    OmBusMpConsumerStatsLine consumer_stats;
} OmBusMpHeader;

typedef struct OmBusMpProducerCounter {
    _Atomic uint64_t published;
    _Atomic uint64_t full_rejects;
    _Atomic uint64_t poisoned_rejects;
    uint8_t _pad[40];
} OmBusMpProducerCounter;

typedef struct OmBusMpSlot {
    _Atomic uint64_t seq;
    uint32_t producer_id;
    uint16_t payload_len;
    uint16_t _reserved;
    uint64_t sequence;
} OmBusMpSlot;

typedef char OmBusMpAssertHeaderCachelines[
    (sizeof(OmBusMpMetaLine) == OM_BUS_MP_CACHELINE_SIZE
     && sizeof(OmBusMpCursorLine) == OM_BUS_MP_CACHELINE_SIZE
     && sizeof(OmBusMpConsumerCursorLine) == OM_BUS_MP_CACHELINE_SIZE
     && sizeof(OmBusMpProducerStatsLine) == OM_BUS_MP_CACHELINE_SIZE
     && sizeof(OmBusMpConsumerStatsLine) == OM_BUS_MP_CACHELINE_SIZE
     && sizeof(OmBusMpProducerCounter) == OM_BUS_MP_CACHELINE_SIZE)
        ? 1 : -1];

static inline bool _om_bus_mp_is_power_of_two(uint32_t v) {
    return v != 0 && (v & (v - 1U)) == 0U;
}

static inline bool _om_bus_mp_is_aligned(const void *p, size_t align) {
    return (((uintptr_t)p) & (align - 1U)) == 0U;
}

static inline size_t _om_bus_mp_align_up(size_t v, size_t align) {
    return (v + align - 1U) & ~(align - 1U);
}

static inline uint64_t _om_bus_mp_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static inline OmBusMpProducerCounter *_om_bus_mp_counters(void *memory) {
    size_t off = _om_bus_mp_align_up(sizeof(OmBusMpHeader), OM_BUS_MP_CACHELINE_SIZE);
    return (OmBusMpProducerCounter *)((char *)memory + off);
}

static inline size_t _om_bus_mp_slots_offset(uint32_t max_producers) {
    size_t off = _om_bus_mp_align_up(sizeof(OmBusMpHeader), OM_BUS_MP_CACHELINE_SIZE);
    off += (size_t)max_producers * sizeof(OmBusMpProducerCounter);
    return _om_bus_mp_align_up(off, OM_BUS_MP_CACHELINE_SIZE);
}

static inline OmBusMpSlot *_om_bus_mp_slot(void *slots, uint32_t slot_size, uint64_t idx) {
    return (OmBusMpSlot *)((char *)slots + idx * slot_size);
}

static inline void _om_bus_mp_notify_publish(OmBusMpHeader *hdr) {
    if (atomic_load_explicit(&hdr->meta.waiters, memory_order_acquire) == 0U) {
        return;
    }
    atomic_fetch_add_explicit(&hdr->meta.notify_seq, 1U, memory_order_release);
#ifdef __linux__
    (void)syscall(SYS_futex, (uint32_t *)&hdr->meta.notify_seq, FUTEX_WAKE,
                  1, NULL, NULL, 0);
#endif
}

static int _om_bus_mp_futex_wait(_Atomic uint32_t *addr, uint32_t expected,
                                 const struct timespec *timeout) {
#ifdef __linux__
    return (int)syscall(SYS_futex, (uint32_t *)addr, FUTEX_WAIT, expected,
                        timeout, NULL, 0);
#else
    (void)addr;
    (void)expected;
    if (timeout) nanosleep(timeout, NULL);
    errno = ETIMEDOUT;
    return -1;
#endif
}

/* Hint the CPU to pull a slot's cacheline ahead of use. Mainly helps when
 * draining a backlog whose slots have fallen out of cache; a no-op where the
 * builtin is unavailable. */
static inline void _om_bus_mp_prefetch(const void *p) {
#if defined(__GNUC__) || defined(__clang__)
    __builtin_prefetch(p, 0 /* read */, 3 /* high temporal locality */);
#else
    (void)p;
#endif
}

static int _om_bus_mp_validate_config(const OmBusMpConfig *config) {
    if (!config) return OM_BUS_MP_ERR_INIT;
    if (!_om_bus_mp_is_power_of_two(config->capacity)) return OM_BUS_MP_ERR_NOT_POW2;
    if (config->slot_size < OM_BUS_MP_SLOT_HEADER_SIZE + 1U) return OM_BUS_MP_ERR_INIT;
    if ((config->slot_size % OM_BUS_MP_CACHELINE_SIZE) != 0U) {
        return OM_BUS_MP_ERR_ALIGNMENT;
    }
    if (config->max_producers == 0) return OM_BUS_MP_ERR_INIT;
    return 0;
}

static int _om_bus_mp_validate_header(const OmBusMpHeader *hdr) {
    if (!hdr) return OM_BUS_MP_ERR_INIT;
    if (!_om_bus_mp_is_aligned(hdr, OM_BUS_MP_CACHELINE_SIZE)) return OM_BUS_MP_ERR_ALIGNMENT;
    if (hdr->meta.magic != OM_BUS_MP_MAGIC) return OM_BUS_MP_ERR_MAGIC;
    if (hdr->meta.version != OM_BUS_MP_VERSION) return OM_BUS_MP_ERR_VERSION;
    return 0;
}

size_t om_bus_mp_size(const OmBusMpConfig *config) {
    if (_om_bus_mp_validate_config(config) != 0) return 0;

    size_t slots_offset = _om_bus_mp_slots_offset(config->max_producers);
    return slots_offset + (size_t)config->capacity * config->slot_size;
}

int om_bus_mp_init(void *memory, size_t memory_size, const OmBusMpConfig *config) {
    if (!memory) return OM_BUS_MP_ERR_INIT;
    int rc = _om_bus_mp_validate_config(config);
    if (rc != 0) return rc;
    if (!_om_bus_mp_is_aligned(memory, OM_BUS_MP_CACHELINE_SIZE)) {
        return OM_BUS_MP_ERR_ALIGNMENT;
    }

    size_t required = om_bus_mp_size(config);
    if (memory_size < required) return OM_BUS_MP_ERR_INIT;

    memset(memory, 0, required);

    OmBusMpHeader *hdr = (OmBusMpHeader *)memory;
    hdr->meta.magic = OM_BUS_MP_MAGIC;
    hdr->meta.version = OM_BUS_MP_VERSION;
    hdr->meta.capacity = config->capacity;
    hdr->meta.slot_size = config->slot_size;
    hdr->meta.max_producers = config->max_producers;
    hdr->meta.skip_timeout_ns = config->skip_timeout_ns
        ? config->skip_timeout_ns : OM_BUS_MP_DEFAULT_SKIP_TIMEOUT_NS;
    atomic_init(&hdr->meta.notify_seq, 0U);
    atomic_init(&hdr->meta.waiters, 0U);
    atomic_init(&hdr->producer_cursor.enqueue_pos, 0U);
    atomic_init(&hdr->consumer_cursor.dequeue_pos, 0U);
    atomic_init(&hdr->producer_stats.records_published, 0U);
    atomic_init(&hdr->producer_stats.full_rejects, 0U);
    atomic_init(&hdr->producer_stats.poisoned_rejects, 0U);
    atomic_init(&hdr->consumer_stats.skipped_sequences, 0U);

    OmBusMpProducerCounter *counters = _om_bus_mp_counters(memory);
    for (uint32_t i = 0; i < config->max_producers; i++) {
        atomic_init(&counters[i].published, 0U);
        atomic_init(&counters[i].full_rejects, 0U);
        atomic_init(&counters[i].poisoned_rejects, 0U);
    }

    void *slots = (char *)memory + _om_bus_mp_slots_offset(config->max_producers);
    for (uint32_t i = 0; i < config->capacity; i++) {
        OmBusMpSlot *slot = _om_bus_mp_slot(slots, config->slot_size, i);
        atomic_init(&slot->seq, (uint64_t)i);
    }

    return 0;
}

int om_bus_mp_producer_open(OmBusMpProducer *producer, void *memory, uint32_t producer_id) {
    if (!producer || !memory) return OM_BUS_MP_ERR_INIT;
    OmBusMpHeader *hdr = (OmBusMpHeader *)memory;
    int rc = _om_bus_mp_validate_header(hdr);
    if (rc != 0) return rc;
    if (producer_id >= hdr->meta.max_producers) return OM_BUS_MP_ERR_PRODUCER_ID;

    memset(producer, 0, sizeof(*producer));
    producer->base = memory;
    producer->slots = (char *)memory + _om_bus_mp_slots_offset(hdr->meta.max_producers);
    producer->counter = &_om_bus_mp_counters(memory)[producer_id];
    producer->producer_id = producer_id;
    producer->capacity = hdr->meta.capacity;
    producer->mask = hdr->meta.capacity - 1U;
    producer->slot_size = hdr->meta.slot_size;
    producer->max_payload = hdr->meta.slot_size - OM_BUS_MP_SLOT_HEADER_SIZE;
    return 0;
}

int om_bus_mp_consumer_open(OmBusMpConsumer *consumer, void *memory) {
    if (!consumer || !memory) return OM_BUS_MP_ERR_INIT;
    OmBusMpHeader *hdr = (OmBusMpHeader *)memory;
    int rc = _om_bus_mp_validate_header(hdr);
    if (rc != 0) return rc;

    memset(consumer, 0, sizeof(*consumer));
    consumer->base = memory;
    consumer->slots = (char *)memory + _om_bus_mp_slots_offset(hdr->meta.max_producers);
    consumer->capacity = hdr->meta.capacity;
    consumer->mask = hdr->meta.capacity - 1U;
    consumer->slot_size = hdr->meta.slot_size;
    consumer->skip_timeout_ns = hdr->meta.skip_timeout_ns;
    consumer->blocked_sequence = UINT64_MAX;
    return 0;
}

int om_bus_mp_claim(OmBusMpProducer *producer, uint16_t payload_len,
                    OmBusMpClaim *claim) {
    if (!producer || !claim) return OM_BUS_MP_ERR_INIT;
    if (payload_len > producer->max_payload) return OM_BUS_MP_ERR_RECORD_TOO_LARGE;

    OmBusMpHeader *hdr = (OmBusMpHeader *)producer->base;
    OmBusMpProducerCounter *counter = (OmBusMpProducerCounter *)producer->counter;
    uint64_t pos = 0;
    OmBusMpSlot *slot = NULL;

    while (true) {
        pos = atomic_load_explicit(&hdr->producer_cursor.enqueue_pos,
                                   memory_order_relaxed);
        slot = _om_bus_mp_slot(producer->slots, producer->slot_size, pos & producer->mask);
        uint64_t seq = atomic_load_explicit(&slot->seq, memory_order_acquire);
        int64_t diff = (int64_t)(seq - pos);

        if (diff == 0) {
            uint64_t next = pos + 1U;
            if (atomic_compare_exchange_weak_explicit(&hdr->producer_cursor.enqueue_pos,
                                                      &pos, next,
                                                      memory_order_relaxed,
                                                      memory_order_relaxed)) {
                break;
            }
        } else if (diff < 0) {
            atomic_fetch_add_explicit(&hdr->producer_stats.full_rejects, 1U,
                                      memory_order_relaxed);
            atomic_fetch_add_explicit(&counter->full_rejects, 1U, memory_order_relaxed);
            return OM_BUS_MP_ERR_FULL;
        }
    }

    slot->sequence = pos;
    slot->producer_id = producer->producer_id;
    slot->payload_len = payload_len;

    claim->sequence = pos;
    claim->producer_id = producer->producer_id;
    claim->payload_len = payload_len;
    claim->max_payload = (uint16_t)producer->max_payload;
    claim->payload = (char *)slot + OM_BUS_MP_SLOT_HEADER_SIZE;
    claim->slot = slot;
    return 0;
}

int om_bus_mp_commit(OmBusMpProducer *producer, OmBusMpClaim *claim) {
    if (!producer || !claim || !claim->slot) return OM_BUS_MP_ERR_INIT;

    OmBusMpHeader *hdr = (OmBusMpHeader *)producer->base;
    OmBusMpProducerCounter *counter = (OmBusMpProducerCounter *)producer->counter;
    OmBusMpSlot *slot = (OmBusMpSlot *)claim->slot;
    uint64_t pos = claim->sequence;

    uint64_t expect = pos;
    if (!atomic_compare_exchange_strong_explicit(&slot->seq, &expect, pos + 1U,
                                                 memory_order_release,
                                                 memory_order_acquire)) {
        atomic_fetch_add_explicit(&hdr->producer_stats.poisoned_rejects, 1U,
                                  memory_order_relaxed);
        atomic_fetch_add_explicit(&counter->poisoned_rejects, 1U, memory_order_relaxed);
        return OM_BUS_MP_ERR_SLOT_POISONED;
    }

    atomic_fetch_add_explicit(&hdr->producer_stats.records_published, 1U,
                              memory_order_relaxed);
    atomic_fetch_add_explicit(&counter->published, 1U, memory_order_relaxed);
    _om_bus_mp_notify_publish(hdr);
    return 0;
}

int om_bus_mp_publish(OmBusMpProducer *producer, const void *payload,
                      uint16_t payload_len, uint64_t *sequence_out) {
    if (!producer || (!payload && payload_len > 0)) return OM_BUS_MP_ERR_INIT;

    OmBusMpClaim claim;
    int rc = om_bus_mp_claim(producer, payload_len, &claim);
    if (rc != 0) return rc;

    if (payload_len > 0) {
        memcpy(claim.payload, payload, payload_len);
    }
    rc = om_bus_mp_commit(producer, &claim);
    if (rc == 0 && sequence_out) *sequence_out = claim.sequence;
    return rc;
}

int om_bus_mp_poll(OmBusMpConsumer *consumer, OmBusMpRecord *record) {
    if (!consumer || !record) return OM_BUS_MP_ERR_INIT;

    OmBusMpHeader *hdr = (OmBusMpHeader *)consumer->base;
    uint64_t pos = atomic_load_explicit(&hdr->consumer_cursor.dequeue_pos,
                                        memory_order_relaxed);
    OmBusMpSlot *slot = _om_bus_mp_slot(consumer->slots, consumer->slot_size,
                                        pos & consumer->mask);
    uint64_t seq = atomic_load_explicit(&slot->seq, memory_order_acquire);

    if (seq == pos + 1U) {
        record->sequence = slot->sequence;
        record->producer_id = slot->producer_id;
        record->payload_len = slot->payload_len;
        record->payload = (const char *)slot + OM_BUS_MP_SLOT_HEADER_SIZE;

        atomic_store_explicit(&slot->seq, pos + consumer->capacity, memory_order_release);
        atomic_store_explicit(&hdr->consumer_cursor.dequeue_pos, pos + 1U,
                              memory_order_release);
        consumer->blocked_sequence = UINT64_MAX;
        consumer->blocked_since_ns = 0;
        return OM_BUS_MP_POLL_RECORD;
    }

    if (seq == pos) {
        uint64_t enqueue_pos = atomic_load_explicit(&hdr->producer_cursor.enqueue_pos,
                                                    memory_order_acquire);
        if (enqueue_pos <= pos) return OM_BUS_MP_POLL_EMPTY;

        uint64_t now = _om_bus_mp_now_ns();
        if (consumer->blocked_sequence != pos) {
            consumer->blocked_sequence = pos;
            consumer->blocked_since_ns = now;
            return OM_BUS_MP_POLL_EMPTY;
        }
        if ((now - consumer->blocked_since_ns) < consumer->skip_timeout_ns) {
            return OM_BUS_MP_POLL_EMPTY;
        }

        uint64_t expect = pos;
        if (atomic_compare_exchange_strong_explicit(&slot->seq, &expect,
                                                    pos + consumer->capacity,
                                                    memory_order_release,
                                                    memory_order_acquire)) {
            atomic_store_explicit(&hdr->consumer_cursor.dequeue_pos, pos + 1U,
                                  memory_order_release);
            atomic_fetch_add_explicit(&hdr->consumer_stats.skipped_sequences, 1U,
                                      memory_order_relaxed);
            consumer->blocked_sequence = UINT64_MAX;
            consumer->blocked_since_ns = 0;
            record->sequence = pos;
            record->producer_id = UINT32_MAX;
            record->payload_len = 0;
            record->payload = NULL;
            return OM_BUS_MP_POLL_SKIPPED;
        }
    }

    return OM_BUS_MP_POLL_EMPTY;
}

int om_bus_mp_wait(OmBusMpConsumer *consumer, uint64_t timeout_ns) {
    if (!consumer || !consumer->base) return OM_BUS_MP_ERR_INIT;

    OmBusMpHeader *hdr = (OmBusMpHeader *)consumer->base;
    if (om_bus_mp_pending(consumer) > 0U) return OM_BUS_MP_WAIT_READY;

    uint32_t expected = atomic_load_explicit(&hdr->meta.notify_seq,
                                             memory_order_acquire);
    atomic_fetch_add_explicit(&hdr->meta.waiters, 1U, memory_order_acq_rel);
    if (om_bus_mp_pending(consumer) > 0U) {
        atomic_fetch_sub_explicit(&hdr->meta.waiters, 1U, memory_order_acq_rel);
        return OM_BUS_MP_WAIT_READY;
    }
    if (timeout_ns == 0U) {
        atomic_fetch_sub_explicit(&hdr->meta.waiters, 1U, memory_order_acq_rel);
        return OM_BUS_MP_WAIT_TIMEOUT;
    }

    struct timespec ts;
    struct timespec *ts_ptr = NULL;
    if (timeout_ns != UINT64_MAX) {
        ts.tv_sec = (time_t)(timeout_ns / 1000000000ULL);
        ts.tv_nsec = (long)(timeout_ns % 1000000000ULL);
        ts_ptr = &ts;
    }

    int rc = _om_bus_mp_futex_wait(&hdr->meta.notify_seq, expected, ts_ptr);
    atomic_fetch_sub_explicit(&hdr->meta.waiters, 1U, memory_order_acq_rel);
    if (rc == 0) return OM_BUS_MP_WAIT_READY;
    if (errno == EAGAIN) return OM_BUS_MP_WAIT_READY;
    if (errno == ETIMEDOUT) return OM_BUS_MP_WAIT_TIMEOUT;
    if (errno == EINTR) return OM_BUS_MP_WAIT_INTERRUPTED;
    return OM_BUS_MP_WAIT_READY;
}

int om_bus_mp_poll_batch(OmBusMpConsumer *consumer, OmBusMpRecord *records,
                         uint32_t max_records) {
    if (!consumer || !records) return OM_BUS_MP_ERR_INIT;
    if (max_records == 0U) return 0;

    OmBusMpHeader *hdr = (OmBusMpHeader *)consumer->base;
    uint64_t pos = atomic_load_explicit(&hdr->consumer_cursor.dequeue_pos,
                                        memory_order_relaxed);
    uint32_t n = 0;

    while (n < max_records) {
        OmBusMpSlot *slot = _om_bus_mp_slot(consumer->slots, consumer->slot_size,
                                            pos & consumer->mask);
        uint64_t seq = atomic_load_explicit(&slot->seq, memory_order_acquire);
        /* Stop at the first slot that is not a committed record. Skip-timeout
         * recovery for a stuck producer is intentionally left to
         * om_bus_mp_poll() so the time-based logic lives in one place. */
        if (seq != pos + 1U) break;

        /* Warm the next slot's cacheline while we copy this one. */
        _om_bus_mp_prefetch(_om_bus_mp_slot(consumer->slots, consumer->slot_size,
                                            (pos + 1U) & consumer->mask));

        OmBusMpRecord *rec = &records[n];
        rec->sequence = slot->sequence;
        rec->producer_id = slot->producer_id;
        rec->payload_len = slot->payload_len;
        rec->payload = (const char *)slot + OM_BUS_MP_SLOT_HEADER_SIZE;

        /* Release the slot for producer reuse, then advance the cursor. Both
         * stores happen per record so crash semantics match om_bus_mp_poll():
         * the consumed/dequeue window is never wider than a single slot. */
        atomic_store_explicit(&slot->seq, pos + consumer->capacity,
                              memory_order_release);
        pos += 1U;
        atomic_store_explicit(&hdr->consumer_cursor.dequeue_pos, pos,
                              memory_order_release);
        n++;
    }

    if (n > 0U) {
        consumer->blocked_sequence = UINT64_MAX;
        consumer->blocked_since_ns = 0;
    }
    return (int)n;
}

uint64_t om_bus_mp_pending(const OmBusMpConsumer *consumer) {
    if (!consumer || !consumer->base) return 0;
    const OmBusMpHeader *hdr = (const OmBusMpHeader *)consumer->base;
    uint64_t enq = atomic_load_explicit(&hdr->producer_cursor.enqueue_pos,
                                        memory_order_acquire);
    uint64_t deq = atomic_load_explicit(&hdr->consumer_cursor.dequeue_pos,
                                        memory_order_relaxed);
    return (enq > deq) ? (enq - deq) : 0U;
}

void om_bus_mp_stats(const void *memory, OmBusMpStats *out) {
    if (!memory || !out) return;
    const OmBusMpHeader *hdr = (const OmBusMpHeader *)memory;
    out->enqueue_pos = atomic_load_explicit(&hdr->producer_cursor.enqueue_pos,
                                            memory_order_relaxed);
    out->dequeue_pos = atomic_load_explicit(&hdr->consumer_cursor.dequeue_pos,
                                            memory_order_relaxed);
    out->records_published = atomic_load_explicit(&hdr->producer_stats.records_published,
                                                  memory_order_relaxed);
    out->full_rejects = atomic_load_explicit(&hdr->producer_stats.full_rejects,
                                             memory_order_relaxed);
    out->poisoned_rejects = atomic_load_explicit(&hdr->producer_stats.poisoned_rejects,
                                                 memory_order_relaxed);
    out->skipped_sequences = atomic_load_explicit(&hdr->consumer_stats.skipped_sequences,
                                                  memory_order_relaxed);
}

uint64_t om_bus_mp_producer_published(const void *memory, uint32_t producer_id) {
    if (!memory) return 0;
    const OmBusMpHeader *hdr = (const OmBusMpHeader *)memory;
    if (_om_bus_mp_validate_header(hdr) != 0 || producer_id >= hdr->meta.max_producers) {
        return 0;
    }
    const OmBusMpProducerCounter *counters =
        (const OmBusMpProducerCounter *)_om_bus_mp_counters((void *)memory);
    return atomic_load_explicit(&counters[producer_id].published, memory_order_relaxed);
}

const char *om_bus_mp_error_string(int err) {
    switch (err) {
        case OM_BUS_MP_OK: return "Success";
        case OM_BUS_MP_ERR_INIT: return "BusMP initialization failed";
        case OM_BUS_MP_ERR_NOT_POW2: return "BusMP capacity is not power of two";
        case OM_BUS_MP_ERR_RECORD_TOO_LARGE: return "BusMP record too large";
        case OM_BUS_MP_ERR_FULL: return "BusMP ring full";
        case OM_BUS_MP_ERR_PRODUCER_ID: return "BusMP invalid producer id";
        case OM_BUS_MP_ERR_MAGIC: return "BusMP magic mismatch";
        case OM_BUS_MP_ERR_VERSION: return "BusMP version mismatch";
        case OM_BUS_MP_ERR_SLOT_POISONED: return "BusMP slot poisoned";
        case OM_BUS_MP_ERR_ALIGNMENT: return "BusMP memory or slot alignment invalid";
        default: return "BusMP unknown error";
    }
}
