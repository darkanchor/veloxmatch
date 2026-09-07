#ifndef OM_WAL_H
#define OM_WAL_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/* Forward declaration - defined in om_slab.h */
struct OmSlabSlot;

/* 
 * High-performance WAL optimized for maximum write throughput
 * Now includes BOTH fixed slab (hot+secondary hot) and aux slab (cold) data
 * Design goals: <200ns per write, 5M+ writes/sec per core
 */

/* WAL record types */
typedef enum OmWalType {
    OM_WAL_INSERT = 1,      /* Variable size: includes fixed+aux data */
    OM_WAL_CANCEL = 2,      /* 32 bytes */
    OM_WAL_MATCH = 3,       /* 48 bytes */
    OM_WAL_CHECKPOINT = 4,  /* 32 bytes */
    OM_WAL_DEACTIVATE = 5,  /* 32 bytes */
    OM_WAL_ACTIVATE = 6,    /* 32 bytes */
    OM_WAL_USER_BASE = 0x80 /* User-defined record base */
} OmWalType;

/* Compact record header - 8 bytes */
typedef struct OmWalHeader {
    uint64_t seq_type_len;  /* Packed: sequence (40 bits) | type (8 bits) | len (16 bits) */
} OmWalHeader;

/* Insert record header - describes the variable-length data that follows */
/* Total header: 64 bytes, followed by: user_data + aux_data */
typedef struct OmWalInsert {
    /* Core order fields (from OmSlabSlot mandatory fields) */
    uint64_t order_id;          /* 8 bytes - unique order ID */
    uint64_t price;             /* 8 bytes - order price */
    uint64_t volume;            /* 8 bytes - original volume */
    uint64_t vol_remain;        /* 8 bytes - remaining volume */
    uint16_t org;               /* 2 bytes - organization ID */
    uint16_t flags;             /* 2 bytes - order flags (side, type, etc.) */
    uint16_t product_id;        /* 2 bytes - product ID */
    uint16_t reserved;          /* 2 bytes - padding */
    
    /* Data sizes for variable-length payload that follows */
    uint32_t user_data_size;    /* 4 bytes - size of secondary hot data (from slab A) */
    uint32_t aux_data_size;     /* 4 bytes - size of cold data (from slab B) */
    
    /* Timestamp */
    uint64_t timestamp_ns;      /* 8 bytes - write timestamp */
    
    /* 
     * Variable-length payload follows this header in WAL:
     * - user_data[user_data_size] from om_slot_get_data(slot)
     * - aux_data[aux_data_size] from om_slot_get_aux_data(&slab, slot)
     * 
     * Total record size = sizeof(OmWalHeader) + sizeof(OmWalInsert) + user_data_size + aux_data_size
     * Aligned to 8-byte boundary
     */
} OmWalInsert;

/* Cancel record - total 32 bytes */
typedef struct OmWalCancel {
    uint64_t order_id;          /* 8 bytes - order being cancelled */
    uint64_t timestamp_ns;      /* 8 bytes - cancel timestamp */
    uint32_t slot_idx;          /* 4 bytes - slot index being freed */
    uint16_t product_id;        /* 2 bytes - product ID */
    uint16_t reserved;          /* 2 bytes - padding */
    /* Total payload: 24 bytes + 8 byte header = 32 bytes */
} OmWalCancel;

/* Deactivate record - total 32 bytes */
typedef struct OmWalDeactivate {
    uint64_t order_id;          /* 8 bytes - order being deactivated */
    uint64_t timestamp_ns;      /* 8 bytes - deactivate timestamp */
    uint32_t slot_idx;          /* 4 bytes - slot index being removed */
    uint16_t product_id;        /* 2 bytes - product ID */
    uint16_t reserved;          /* 2 bytes - padding */
} OmWalDeactivate;

/* Activate record - total 32 bytes */
typedef struct OmWalActivate {
    uint64_t order_id;          /* 8 bytes - order being activated */
    uint64_t timestamp_ns;      /* 8 bytes - activate timestamp */
    uint32_t slot_idx;          /* 4 bytes - slot index being activated */
    uint16_t product_id;        /* 2 bytes - product ID */
    uint16_t reserved;          /* 2 bytes - padding */
} OmWalActivate;

/* Match record - total 48 bytes */
typedef struct OmWalMatch {
    uint64_t maker_id;          /* 8 bytes - maker order ID */
    uint64_t taker_id;          /* 8 bytes - taker order ID */
    uint64_t price;             /* 8 bytes - execution price */
    uint64_t volume;            /* 8 bytes - volume traded */
    uint64_t timestamp_ns;      /* 8 bytes - trade timestamp */
    uint16_t product_id;        /* 2 bytes - product ID */
    uint16_t reserved[3];       /* 6 bytes - padding */
    /* Total payload: 40 bytes + 8 byte header = 48 bytes */
} OmWalMatch;

/* WAL configuration - now includes data sizes for variable-length records */
typedef struct OmWalConfig {
    const char *filename;       /* WAL file path */
    const char *filename_pattern; /* Optional pattern for multi-file WAL ("/path/wal_%06u.log") */
    uint32_t file_index;         /* Starting file index for multi-file WAL */
    size_t buffer_size;         /* Write buffer size (default 1MB) */
    uint32_t sync_interval_ms;  /* Fsync interval in ms (default 10) */
    bool use_direct_io;         /* Use O_DIRECT (default true) */
    bool enable_crc32;          /* CRC32 on write (default true) */
    bool disable_crc32;         /* Explicit opt-out: set true to disable CRC32 */
    
    /* Data sizes - must match slab configuration */
    size_t user_data_size;      /* Size of secondary hot data (from OmSlabConfig.user_data_size) */
    size_t aux_data_size;       /* Size of cold data (from OmSlabConfig.aux_data_size) */

    uint64_t wal_max_file_size; /* Max WAL size before next file (0 = unlimited) */
} OmWalConfig;

/* Forward declaration for slab */
struct OmDualSlab;

/* WAL context */
typedef struct OmWal {
    int fd;                     /* File descriptor (O_DIRECT if enabled) */
    int last_error;             /* Sticky writer failure; never retry a torn append */
    void *buffer;               /* Write buffer (aligned for O_DIRECT) */
    void *buffer_unaligned;     /* Original malloc pointer for freeing */
    size_t buffer_size;         /* Total buffer size */
    size_t buffer_used;         /* Bytes used in current buffer */
    uint64_t sequence;          /* Next sequence number */
    uint64_t file_offset;       /* Current file offset */
    uint32_t file_index;         /* Current WAL file index */
    OmWalConfig config;         /* Configuration copy with data sizes */
    struct OmDualSlab *slab;    /* Slab pointer for aux data access (can be NULL) */

    /* Post-write callback - invoked after each WAL write with record data */
    void (*post_write)(uint64_t seq, uint8_t type, const void *data,
                       uint16_t len, void *ctx);
    void *post_write_ctx;
} OmWal;

/* Initialize WAL with high-performance settings */
int om_wal_init(OmWal *wal, const OmWalConfig *config);

/* Close WAL and free resources */
void om_wal_close(OmWal *wal);

/**
 * Set slab pointer for aux data access during insert logging.
 * Must be called after om_wal_init and before om_wal_insert if aux_data_size > 0.
 * @param wal WAL context
 * @param slab Dual slab pointer (can be NULL to disable aux data logging)
 */
void om_wal_set_slab(OmWal *wal, struct OmDualSlab *slab);

/**
 * Set post-write callback. Invoked after each WAL write with record data.
 * @param wal WAL context
 * @param fn  Callback: (seq, type, data, len, ctx)
 * @param ctx User context passed to callback
 */
void om_wal_set_post_write(OmWal *wal,
    void (*fn)(uint64_t, uint8_t, const void*, uint16_t, void*), void *ctx);

/* Write operations - all return sequence number on success, 0 on failure */
/* These are FAST PATH - just append to buffer, no syscalls, no locks */

/* 
 * Log insert to WAL - stores BOTH fixed slot data and aux data
 * @param wal WAL context
 * @param slot Order slot with all data
 * @param product_id Product ID for this order
 * @return Sequence number on success, 0 on failure
 * 
 * Stores:
 * - OmSlabSlot mandatory fields (price, volume, etc.)
 * - User data (secondary hot data from slot->data[])
 * - Aux data (cold data from slab_b)
 */
uint64_t om_wal_insert(OmWal *wal, struct OmSlabSlot *slot, uint16_t product_id);

/* Log cancel to WAL */
uint64_t om_wal_cancel(OmWal *wal, uint32_t order_id, uint32_t slot_idx, uint16_t product_id);

/* Log deactivate to WAL */
uint64_t om_wal_deactivate(OmWal *wal, uint32_t order_id, uint32_t slot_idx, uint16_t product_id);

/* Log activate to WAL */
uint64_t om_wal_activate(OmWal *wal, uint32_t order_id, uint32_t slot_idx, uint16_t product_id);

/* Log match to WAL */
uint64_t om_wal_match(OmWal *wal, const OmWalMatch *rec);

/* Flush buffer to disk - call periodically or when buffer is full */
int om_wal_flush(OmWal *wal);

/* Force fsync - call on checkpoint or graceful shutdown */
int om_wal_fsync(OmWal *wal);

/* Get current sequence number */
static inline uint64_t om_wal_sequence(const OmWal *wal) {
    return wal ? wal->sequence : 0;
}

/* Calculate packed header value */
static inline uint64_t om_wal_pack_header(uint64_t seq, uint8_t type, uint16_t len) {
    return (seq << 24) | ((uint64_t)type << 16) | len;
}

/* Extract from packed header */
static inline uint64_t om_wal_header_seq(uint64_t packed) { return packed >> 24; }
static inline uint8_t om_wal_header_type(uint64_t packed) { return (packed >> 16) & 0xFF; }
static inline uint16_t om_wal_header_len(uint64_t packed) { return packed & 0xFFFF; }

/* ============================================================================
 * WAL REPLAY / RECOVERY
 * ============================================================================ */

/* Replay iterator for scanning WAL files */
typedef struct OmWalReplay {
    int fd;                     /* File descriptor for reading */
    void *buffer;               /* Read buffer (4KB aligned) */
    void *buffer_unaligned;     /* Original allocation pointer */
    size_t buffer_size;         /* Buffer size (typically 1MB) */
    size_t buffer_valid;        /* Valid bytes in buffer */
    size_t buffer_pos;          /* Current read position in buffer */
    uint64_t file_offset;       /* Current offset in file */
    uint64_t file_size;         /* Total file size */
    uint32_t file_index;         /* Current WAL file index */
    uint64_t last_sequence;     /* Last sequence number read */
    bool eof;                   /* End of file reached */
    const char *filename_pattern; /* Optional pattern for multi-file replay */
    
    /* Data sizes from WAL config - needed for parsing insert records */
    size_t user_data_size;
    size_t aux_data_size;
    bool enable_crc32;          /* Whether to validate CRC32 on replay */

    /* CRC mismatch details — populated when replay_next returns OM_ERR_WAL_CRC_MISMATCH */
    uint32_t last_stored_crc;   /* CRC read from file (bad value) */
    uint32_t last_computed_crc; /* CRC computed over header+payload (good value) */
    uint64_t last_record_offset; /* File byte offset of the bad record header */

    void *user_ctx;             /* User-defined context for custom records */
    int (*user_handler)(OmWalType type, const void *data, size_t len, void *user_ctx);
} OmWalReplay;

/* Initialize replay iterator for reading WAL file */
int om_wal_replay_init(OmWalReplay *replay, const char *filename);

/* Initialize replay with data sizes (needed for parsing INSERT records) */
int om_wal_replay_init_with_sizes(OmWalReplay *replay, const char *filename,
                                  size_t user_data_size, size_t aux_data_size);

/* Initialize replay with full config (needed for CRC32 validation and data sizes) */
int om_wal_replay_init_with_config(OmWalReplay *replay, const char *filename,
                                    const OmWalConfig *config);

/* Register user record handler for replay (type >= OM_WAL_USER_BASE) */
void om_wal_replay_set_user_handler(OmWalReplay *replay,
                                    int (*handler)(OmWalType type, const void *data, size_t len, void *user_ctx),
                                    void *user_ctx);

/* Close replay iterator */
void om_wal_replay_close(OmWalReplay *replay);

/* Read next record from WAL during replay */
/* Returns: 1 = success, 0 = EOF, -1 = error, -2 = CRC mismatch */
/* For INSERT records, data_len includes both header + user_data + aux_data */
int om_wal_replay_next(OmWalReplay *replay, OmWalType *type, void **data, 
                       uint64_t *sequence, size_t *data_len);

/* Append a custom WAL record (type >= OM_WAL_USER_BASE) */
uint64_t om_wal_append_custom(OmWal *wal, OmWalType type, const void *data, size_t len);

/* Get statistics about replay progress */
typedef struct OmWalReplayStats {
    uint64_t records_insert;
    uint64_t records_cancel;
    uint64_t records_match;
    uint64_t records_other;
    uint64_t bytes_processed;
    uint64_t last_sequence;
} OmWalReplayStats;

/* Forward declaration - defined in orderbook.h */
struct OmOrderbookContext;

/**
 * Replay WAL file and reconstruct orderbook state with FULL data recovery
 * 
 * This function reconstructs the ENTIRE orderbook state including:
 * - Fixed slab (OmSlabSlot): mandatory fields + secondary hot data
 * - Aux slab (OmSlabB): cold/auxiliary data
 * - Hashmap: order_id -> slot mappings
 * - Price ladders: Q1 price level linked lists
 * - Time queues: Q2 FIFO per price level
 * 
 * @param ctx Orderbook context to reconstruct (must be initialized with same config)
 * @param wal_filename Path to WAL file
 * @param stats Output statistics (can be NULL)
 * @return 0 on success, negative on error
 */
int om_orderbook_recover_from_wal(struct OmOrderbookContext *ctx, 
                                   const char *wal_filename,
                                   OmWalReplayStats *stats);

#endif
