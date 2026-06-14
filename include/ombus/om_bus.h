#ifndef OM_BUS_H
#define OM_BUS_H

/**
 * @file om_bus.h
 * @brief SHM transport for WAL record distribution across process boundaries
 *
 * Provides a shared-memory ring buffer that carries inline WAL record data
 * (not pointers) so consumers in separate processes can read records via mmap.
 *
 * Producer: OmBusStream  — creates SHM, publishes records
 * Consumer: OmBusEndpoint — attaches to SHM, polls records
 */

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "om_bus_error.h"

/* ============================================================================
 * Constants
 * ============================================================================ */

#define OM_BUS_SHM_MAGIC       0x4F4D4253U  /* "OMBS" */
#define OM_BUS_SHM_VERSION     1U
#define OM_BUS_HEADER_PAGE     4096U
#define OM_BUS_SLOT_HEADER_SIZE 24U
#define OM_BUS_CONSUMER_ALIGN  64U
#define OM_BUS_DEFAULT_SLOT_SIZE    256U
#define OM_BUS_DEFAULT_CAPACITY     4096U
#define OM_BUS_DEFAULT_MAX_CONSUMERS 8U

#define OM_BUS_FLAG_CRC              0x1U  /* Enable CRC32 on publish/poll */
#define OM_BUS_FLAG_REJECT_REORDER   0x2U  /* Return error on wal_seq < expected */
#define OM_BUS_FLAG_BROADCAST        0x4U  /* Producer overwrites oldest; consumers detect laps */

/* ============================================================================
 * Slot Header (24 bytes) — sits at the start of each ring slot
 * ============================================================================ */

typedef struct OmBusSlotHeader {
    _Atomic uint64_t seq;       /* Monotonic slot sequence; publish fence */
    uint64_t wal_seq;           /* WAL sequence number */
    uint8_t  wal_type;          /* OmWalType enum value */
    uint8_t  reserved;          /* Alignment padding */
    uint16_t payload_len;       /* Payload byte count */
    uint32_t crc32;             /* CRC32 of payload bytes */
} OmBusSlotHeader;

/* ============================================================================
 * SHM Header (4096 bytes, page-aligned) — first page of SHM file
 * ============================================================================ */

typedef struct OmBusShmHeader {
    uint32_t magic;             /* 0x4F4D4253 ("OMBS") */
    uint32_t version;           /* Protocol version (1) */
    uint32_t slot_size;         /* Bytes per slot (default 256) */
    uint32_t capacity;          /* Number of slots (power of two) */
    uint32_t max_consumers;     /* Maximum consumer count */
    uint32_t flags;             /* Feature flags (OM_BUS_FLAG_CRC, etc.) */
    _Atomic uint64_t head;      /* Producer head position */
    _Atomic uint64_t min_tail;  /* Cached minimum consumer tail */
    _Atomic uint64_t producer_epoch; /* Incremented on each stream_create */
    uint64_t staleness_ns;      /* Consumer staleness threshold for non-broadcast mode */
    char stream_name[64];       /* Null-terminated stream name */
    uint8_t _pad[OM_BUS_HEADER_PAGE - 120];
} OmBusShmHeader;

/* ============================================================================
 * Consumer Tail (64 bytes, cache-line aligned) — one per consumer slot
 * ============================================================================ */

typedef struct OmBusConsumerTail {
    _Atomic uint64_t tail;      /* Consumer read position */
    _Atomic uint64_t wal_seq;   /* Last WAL sequence consumed */
    _Atomic uint64_t last_poll_ns; /* clock_gettime(MONOTONIC) at last poll */
    uint8_t _pad[40];           /* Pad to 64 bytes (cache line) */
} OmBusConsumerTail;

/* ============================================================================
 * Output record — delivered to consumers
 * ============================================================================ */

typedef struct OmBusRecord {
    uint64_t    wal_seq;        /* WAL sequence number */
    uint8_t     wal_type;       /* OmWalType enum value */
    uint16_t    payload_len;    /* Payload byte count */
    const void *payload;        /* Pointer to payload data */
} OmBusRecord;

/* ============================================================================
 * Stream (Producer) API
 * ============================================================================ */

/**
 * Optional callback for sustained backpressure.
 * Called when the producer has been spinning for >42 iterations (Phase 3).
 */
typedef void (*OmBusBackpressureCb)(uint64_t head, uint64_t min_tail, void *ctx);

typedef struct OmBusStreamConfig {
    const char *stream_name;    /* SHM object name (e.g., "/om-bus-engine-0") */
    uint32_t    capacity;       /* Ring capacity, power of two (default 4096) */
    uint32_t    slot_size;      /* Bytes per slot (default 256) */
    uint32_t    max_consumers;  /* Maximum consumer count (default 8) */
    uint32_t    flags;          /* Feature flags (OM_BUS_FLAG_CRC, etc.) */
    uint64_t    staleness_ns;   /* Consumer staleness threshold (0 = disabled, default 5s) */
    OmBusBackpressureCb backpressure_cb;  /* Optional backpressure callback */
    void       *backpressure_ctx;         /* User context for callback */
} OmBusStreamConfig;

typedef struct OmBusStream OmBusStream;

/**
 * Create a new SHM stream (producer side).
 * Creates the shared memory file and initializes the ring.
 * @param out    Output stream handle
 * @param config Stream configuration
 * @return 0 on success, negative on error
 */
int om_bus_stream_create(OmBusStream **out, const OmBusStreamConfig *config);

/**
 * Publish a WAL record to the stream.
 * Copies payload into the next ring slot. Blocks (spins) if ring is full unless
 * OM_BUS_FLAG_BROADCAST is set, in which case the oldest slot may be overwritten.
 * @param stream  Stream handle
 * @param wal_seq WAL sequence number
 * @param wal_type OmWalType enum value
 * @param payload Raw WAL record data
 * @param len     Payload byte count
 * @return 0 on success, OM_ERR_BUS_RECORD_TOO_LARGE if len > slot_size - 24
 */
int om_bus_stream_publish(OmBusStream *stream, uint64_t wal_seq,
                          uint8_t wal_type, const void *payload, uint16_t len);

/**
 * Publish a batch of WAL records to the stream.
 * Amortizes min_tail refresh and head advancement across the batch.
 * @param stream    Stream handle
 * @param recs      Array of records to publish
 * @param count     Number of records
 * @return 0 on success, OM_ERR_BUS_RECORD_TOO_LARGE if any record is too large
 */
int om_bus_stream_publish_batch(OmBusStream *stream, const OmBusRecord *recs,
                                 uint32_t count);

/**
 * Stream statistics snapshot.
 */
typedef struct OmBusStreamStats {
    uint64_t records_published;      /* total records published */
    uint64_t head;                   /* current head position */
    uint64_t min_tail;               /* current minimum consumer tail */
} OmBusStreamStats;

/**
 * Snapshot current stream statistics.
 */
void om_bus_stream_stats(const OmBusStream *s, OmBusStreamStats *out);

/**
 * Destroy stream and unlink SHM object.
 * @param stream Stream handle (NULL-safe)
 */
void om_bus_stream_destroy(OmBusStream *stream);

/* ============================================================================
 * Endpoint (Consumer) API
 * ============================================================================ */

typedef struct OmBusEndpointConfig {
    const char *stream_name;    /* SHM object name to attach to */
    uint32_t    consumer_index; /* Pre-assigned consumer index */
    bool        zero_copy;      /* If true, payload points into mmap region */
} OmBusEndpointConfig;

typedef struct OmBusEndpoint OmBusEndpoint;

/**
 * Open an endpoint to an existing SHM stream (consumer side).
 * Maps the shared memory file.
 * Zero-copy endpoints are rejected for streams using OM_BUS_FLAG_BROADCAST.
 * @param out    Output endpoint handle
 * @param config Endpoint configuration
 * @return 0 on success, negative on error
 */
int om_bus_endpoint_open(OmBusEndpoint **out, const OmBusEndpointConfig *config);

/**
 * Poll for the next record. Non-blocking.
 * @param ep  Endpoint handle
 * @param rec Output record (payload pointer valid until next poll or close)
 * @return 1 if record available, 0 if empty, negative on error
 *         OM_ERR_BUS_CRC_MISMATCH if CRC check fails
 *         OM_ERR_BUS_LAPPED if this endpoint was overrun in broadcast mode
 */
int om_bus_endpoint_poll(OmBusEndpoint *ep, OmBusRecord *rec);

/**
 * Poll up to max_count records in a batch. Non-blocking.
 * @param ep        Endpoint handle
 * @param recs      Output record array
 * @param max_count Maximum records to return
 * @return Number of records read (0 = empty), negative on error
 */
int om_bus_endpoint_poll_batch(OmBusEndpoint *ep, OmBusRecord *recs,
                               size_t max_count);

/**
 * Move this endpoint's cursor to the current live head.
 * Intended after OM_ERR_BUS_LAPPED in broadcast mode before WAL gap replay.
 * @param ep Endpoint handle
 * @return 0 on success, negative on error
 */
int om_bus_endpoint_seek_head(OmBusEndpoint *ep);

/**
 * Get current WAL sequence position for this endpoint.
 * @param ep Endpoint handle
 * @return Last consumed WAL sequence number
 */
uint64_t om_bus_endpoint_wal_seq(const OmBusEndpoint *ep);

/**
 * Close endpoint and unmap SHM.
 * @param ep Endpoint handle (NULL-safe)
 */
void om_bus_endpoint_close(OmBusEndpoint *ep);

/* ============================================================================
 * Consumer Cursor Persistence
 * ============================================================================ */

#define OM_BUS_CURSOR_MAGIC 0x4F4D4243U  /* "OMBC" */

/**
 * Save current endpoint cursor (WAL seq + tail) to a file.
 * Format: [magic:4][wal_seq:8][crc32:4] = 16 bytes.
 * @param ep   Endpoint handle
 * @param path File path to write
 * @return 0 on success, negative on error
 */
int om_bus_endpoint_save_cursor(const OmBusEndpoint *ep, const char *path);

/**
 * Load a previously saved cursor from file.
 * @param path        File path to read
 * @param wal_seq_out Output WAL sequence number
 * @return 0 on success, negative on error
 */
int om_bus_endpoint_load_cursor(const char *path, uint64_t *wal_seq_out);

#endif /* OM_BUS_H */
