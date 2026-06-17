#ifndef OM_BUS_MP_H
#define OM_BUS_MP_H

/**
 * @file om_bus_mp.h
 * @brief Bounded many-producer/single-consumer ring for C ABI compatible SHM use
 *
 * OmBusMP is a caller-owned-memory queue. Put 64-byte aligned memory in shm,
 * mmap, or a process-local allocation, call om_bus_mp_init() once, then open
 * producer and consumer handles in any C ABI compatible process.
 */

#include <stddef.h>
#include <stdint.h>

#define OM_BUS_MP_MAGIC 0x4F4D4250U  /* "OMBP" */
#define OM_BUS_MP_VERSION 1U
#define OM_BUS_MP_DEFAULT_SKIP_TIMEOUT_NS 100000ULL
#define OM_BUS_MP_CACHELINE_SIZE 64U
#define OM_BUS_MP_PRODUCER_ALIGN OM_BUS_MP_CACHELINE_SIZE
#define OM_BUS_MP_SLOT_HEADER_SIZE 24U

typedef enum OmBusMpError {
    OM_BUS_MP_OK = 0,
    OM_BUS_MP_ERR_INIT = -900,
    OM_BUS_MP_ERR_NOT_POW2 = -901,
    OM_BUS_MP_ERR_RECORD_TOO_LARGE = -902,
    OM_BUS_MP_ERR_FULL = -903,
    OM_BUS_MP_ERR_PRODUCER_ID = -904,
    OM_BUS_MP_ERR_MAGIC = -905,
    OM_BUS_MP_ERR_VERSION = -906,
    OM_BUS_MP_ERR_SLOT_POISONED = -907,
    OM_BUS_MP_ERR_ALIGNMENT = -908,
} OmBusMpError;

typedef enum OmBusMpPollResult {
    OM_BUS_MP_POLL_EMPTY = 0,
    OM_BUS_MP_POLL_RECORD = 1,
    OM_BUS_MP_POLL_SKIPPED = 2,
} OmBusMpPollResult;

typedef struct OmBusMpConfig {
    uint32_t capacity;        /* Ring capacity, must be a power of two */
    uint32_t slot_size;       /* Bytes per slot, including header; cacheline multiple */
    uint32_t max_producers;   /* Number of producer stat slots */
    uint64_t skip_timeout_ns; /* 0 uses OM_BUS_MP_DEFAULT_SKIP_TIMEOUT_NS */
} OmBusMpConfig;

typedef struct OmBusMpProducer {
    void *base;
    void *slots;
    void *counter;
    uint32_t producer_id;
    uint32_t capacity;
    uint32_t mask;
    uint32_t slot_size;
    uint32_t max_payload;
} OmBusMpProducer;

typedef struct OmBusMpConsumer {
    void *base;
    void *slots;
    uint32_t capacity;
    uint32_t mask;
    uint32_t slot_size;
    uint64_t skip_timeout_ns;
    uint64_t blocked_sequence;
    uint64_t blocked_since_ns;
} OmBusMpConsumer;

typedef struct OmBusMpRecord {
    uint64_t sequence;     /* Global queue sequence; authoritative ordering point */
    uint32_t producer_id;
    uint16_t payload_len;
    const void *payload;   /* Points into the ring; valid until slot reuse */
} OmBusMpRecord;

typedef struct OmBusMpClaim {
    uint64_t sequence;
    uint32_t producer_id;
    uint16_t payload_len;
    uint16_t max_payload;
    void *payload;         /* Producer-owned until om_bus_mp_commit() */
    void *slot;
} OmBusMpClaim;

typedef struct OmBusMpStats {
    uint64_t enqueue_pos;
    uint64_t dequeue_pos;
    uint64_t records_published;
    uint64_t full_rejects;
    uint64_t poisoned_rejects;
    uint64_t skipped_sequences;
} OmBusMpStats;

size_t om_bus_mp_size(const OmBusMpConfig *config);
int om_bus_mp_init(void *memory, size_t memory_size, const OmBusMpConfig *config);

int om_bus_mp_producer_open(OmBusMpProducer *producer, void *memory, uint32_t producer_id);
int om_bus_mp_consumer_open(OmBusMpConsumer *consumer, void *memory);

int om_bus_mp_claim(OmBusMpProducer *producer, uint16_t payload_len,
                    OmBusMpClaim *claim);
int om_bus_mp_commit(OmBusMpProducer *producer, OmBusMpClaim *claim);
int om_bus_mp_publish(OmBusMpProducer *producer, const void *payload,
                      uint16_t payload_len, uint64_t *sequence_out);
int om_bus_mp_poll(OmBusMpConsumer *consumer, OmBusMpRecord *record);

/**
 * Drain up to max_records ready records into the caller's array in one call.
 *
 * Semantically equivalent to calling om_bus_mp_poll() repeatedly while it
 * returns OM_BUS_MP_POLL_RECORD, but amortizes the call overhead, tracks the
 * consumer cursor locally, and prefetches the next slot. Intended for the
 * single-consumer drain loop of a latency-sensitive poller (e.g. a matcher).
 *
 * Stops at the first non-ready slot and does NOT perform the skip-timeout
 * recovery that om_bus_mp_poll() does for a stuck/uncommitted producer slot.
 * If a batch returns fewer records than requested and the queue still appears
 * non-empty (see om_bus_mp_pending), call om_bus_mp_poll() once to advance the
 * skip logic.
 *
 * Returns the number of records filled (0..max_records), or a negative
 * OmBusMpError for invalid arguments. As with om_bus_mp_poll(), each record's
 * payload points into the ring and is valid until the slot is reused; since
 * consumed slots are released for producer reuse immediately, max_records must
 * be far smaller than the ring capacity (always true in practice).
 */
int om_bus_mp_poll_batch(OmBusMpConsumer *consumer, OmBusMpRecord *records,
                         uint32_t max_records);

/**
 * Approximate number of records pending for the consumer: enqueue_pos minus
 * dequeue_pos. Cheap (two atomic loads) for use as a queue-lag/depth gauge.
 * Upper bound: it includes slots a producer has claimed but not yet committed.
 * Returns 0 if consumer is NULL/unopened.
 */
uint64_t om_bus_mp_pending(const OmBusMpConsumer *consumer);

void om_bus_mp_stats(const void *memory, OmBusMpStats *out);
uint64_t om_bus_mp_producer_published(const void *memory, uint32_t producer_id);

const char *om_bus_mp_error_string(int err);

#endif /* OM_BUS_MP_H */
