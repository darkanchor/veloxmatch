#define _GNU_SOURCE  /* For O_DIRECT */
#if defined(__APPLE__)
#include <TargetConditionals.h>
#include <AvailabilityMacros.h>
#endif
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <assert.h>

#include "om_wal.h"
#include "om_slab.h"
#include "om_error.h"

/* Align to 4KB for O_DIRECT */
#define WAL_ALIGN 4096
#define WAL_ALIGN_MASK (WAL_ALIGN - 1)

/* Record sizes for fixed-size records */
#define WAL_CANCEL_SIZE sizeof(OmWalCancel)
#define WAL_MATCH_SIZE sizeof(OmWalMatch)
#define WAL_HEADER_SIZE sizeof(OmWalHeader)
#define WAL_CRC32_SIZE 4

/*
 * Get current timestamp in nanoseconds using CLOCK_MONOTONIC.
 * CLOCK_MONOTONIC is used instead of CLOCK_REALTIME because:
 * - It is not affected by NTP adjustments or manual clock changes
 * - It provides consistent, monotonically increasing values
 * - It's ideal for ordering events within a single system run
 * Note: For cross-system timestamp correlation, consider CLOCK_REALTIME.
 */
static inline uint64_t wal_get_timestamp_ns(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0) {
        return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
    }
#if defined(__APPLE__)
    if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
        return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
    }
#endif
    return 0;
}

/* CRC32 lookup table (IEEE 802.3 polynomial: 0xEDB88320) */
static uint32_t crc32_table[256];
static bool crc32_table_initialized = false;

static void crc32_init_table(void) {
    if (crc32_table_initialized) return;
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++) {
            crc = (crc >> 1) ^ ((crc & 1) ? 0xEDB88320 : 0);
        }
        crc32_table[i] = crc;
    }
    crc32_table_initialized = true;
}

static uint32_t crc32_compute(const void *data, size_t len) {
    crc32_init_table();
    const uint8_t *buf = (const uint8_t *)data;
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        crc = crc32_table[(crc ^ buf[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

/* Align pointer up to boundary */
static inline void *align_up(void *ptr, size_t align) {
    uintptr_t p = (uintptr_t)ptr;
    return (void *)((p + align - 1) & ~(align - 1));
}

/* Calculate total insert record size including variable-length data */
static inline size_t wal_insert_total_size(size_t user_data_size, size_t aux_data_size) {
    size_t total = sizeof(OmWalHeader) + sizeof(OmWalInsert) + user_data_size + aux_data_size;
    /* Align to 8-byte boundary */
    return (total + 7) & ~7;
}

/* Get payload size based on type and config (for replay) */
static inline size_t wal_payload_size(OmWalType type, size_t user_data_size, size_t aux_data_size) {
    switch (type) {
        case OM_WAL_INSERT: return sizeof(OmWalInsert) + user_data_size + aux_data_size;
        case OM_WAL_CANCEL: return sizeof(OmWalCancel);
        case OM_WAL_MATCH: return sizeof(OmWalMatch);
        case OM_WAL_CHECKPOINT: return 32;
        default: return 0;
    }
}

/* Sequence discovery shares the integrity/framing reader with recovery. */
static int wal_scan_for_last_sequence(const char *filename, const OmWalConfig *config,
                                     uint64_t *last_seq, uint32_t *last_file) {
    OmWalReplay replay;
    int rc = om_wal_replay_init_with_config(&replay, filename, config);
    if (rc != 0) return rc;
    OmWalType type; void *data; uint64_t seq; size_t len;
    *last_seq = 0;
    *last_file = config->file_index;
    while ((rc = om_wal_replay_next(&replay, &type, &data, &seq, &len)) == 1) {
        *last_seq = seq;
        *last_file = replay.file_index;
    }
    om_wal_replay_close(&replay);
    return rc;
}

static int wal_open_file(OmWal *wal, const char *path) {
    int flags = O_WRONLY | O_CREAT | O_APPEND;
#if defined(__APPLE__)
    if (wal->config.use_direct_io) {
        wal->config.use_direct_io = false;
    }
#else
    if (wal->config.use_direct_io) {
        flags |= O_DIRECT;
    }
#endif
    wal->fd = open(path, flags, 0644);
    if (wal->fd < 0) {
        return OM_ERR_WAL_OPEN;
    }
    return OM_OK;
}

static int wal_open_indexed(OmWal *wal, uint32_t index) {
    if (!wal->config.filename_pattern) {
        return OM_ERR_NULL_PARAM;
    }
    char path[512];
    snprintf(path, sizeof(path), wal->config.filename_pattern, index);
    return wal_open_file(wal, path);
}

int om_wal_init(OmWal *wal, const OmWalConfig *config) {
    if (!wal || !config || !config->filename) {
        return OM_ERR_NULL_PARAM;
    }

    memset(wal, 0, sizeof(OmWal));
    wal->fd = -1;
    wal->config = *config;
    wal->slab = NULL;

    /* CRC32 on by default unless explicitly opted out */
    if (!wal->config.disable_crc32) {
        wal->config.enable_crc32 = true;
    }

    if (wal->config.buffer_size == 0) {
        wal->config.buffer_size = 1024 * 1024;
    }
    wal->config.buffer_size = (wal->config.buffer_size + WAL_ALIGN - 1) & ~WAL_ALIGN_MASK;

    wal->buffer_unaligned = malloc(wal->config.buffer_size + WAL_ALIGN);
    if (!wal->buffer_unaligned) {
        return OM_ERR_WAL_BUFFER_ALLOC;
    }
    wal->buffer = align_up(wal->buffer_unaligned, WAL_ALIGN);
    wal->buffer_size = wal->config.buffer_size;
    wal->buffer_used = 0;

    wal->file_index = wal->config.file_index;
    if (wal->config.filename_pattern) {
        if (wal_open_indexed(wal, wal->file_index) != 0) {
            free(wal->buffer_unaligned);
            return OM_ERR_WAL_OPEN;
        }
    } else {
        if (wal_open_file(wal, config->filename) != 0) {
            free(wal->buffer_unaligned);
            return OM_ERR_WAL_OPEN;
        }
    }

    struct stat st;
    uint64_t last_seq = 0;
    uint32_t last_file = wal->file_index;
    int rc = fstat(wal->fd, &st) == 0 ? 0 : OM_ERR_WAL_READ;
    if (rc == 0 && (st.st_size > 0 || config->filename_pattern))
        rc = wal_scan_for_last_sequence(config->filename, &wal->config, &last_seq, &last_file);
    if (rc == 0 && last_file != wal->file_index) {
        close(wal->fd);
        wal->fd = -1;
        wal->file_index = last_file;
        rc = wal_open_indexed(wal, last_file);
        if (rc == 0 && fstat(wal->fd, &st) != 0) rc = OM_ERR_WAL_READ;
    }
    if (rc != 0) {
        if (wal->fd >= 0) close(wal->fd);
        free(wal->buffer_unaligned);
        wal->buffer_unaligned = NULL;
        wal->fd = -1;
        return rc;
    }
    wal->file_offset = st.st_size;
    wal->sequence = last_seq + 1;

    return 0;
}

void om_wal_set_slab(OmWal *wal, struct OmDualSlab *slab) {
    if (wal) {
        wal->slab = slab;
    }
}

void om_wal_set_post_write(OmWal *wal,
    void (*fn)(uint64_t, uint8_t, const void*, uint16_t, void*), void *ctx) {
    if (wal) {
        wal->post_write = fn;
        wal->post_write_ctx = ctx;
    }
}

void om_wal_close(OmWal *wal) {
    if (!wal) return;

    /* Flush remaining buffer */
    if (wal->buffer_used > 0) {
        om_wal_flush(wal);
    }

    /* Final fsync */
    if (wal->fd >= 0) {
        fsync(wal->fd);
        close(wal->fd);
        wal->fd = -1;
    }

    if (wal->buffer_unaligned) {
        free(wal->buffer_unaligned);
        wal->buffer_unaligned = NULL;
        wal->buffer = NULL;
    }
}

static uint64_t wal_append(OmWal *wal, OmWalType type, const void *data, size_t data_size) {
    if (wal->last_error) return 0;
    size_t crc_size = wal->config.enable_crc32 ? WAL_CRC32_SIZE : 0;
    size_t total_size = WAL_HEADER_SIZE + data_size + crc_size;
    if (data_size > UINT16_MAX || total_size > wal->buffer_size || wal->sequence >= (UINT64_C(1) << 40)) {
        wal->last_error = OM_ERR_WAL_WRITE;
        return 0;
    }
    
    if (wal->buffer_used + total_size > wal->buffer_size) {
        if (om_wal_flush(wal) != 0) {
            return 0;
        }
    }

    uint64_t seq = wal->sequence++;
    char *buf = (char *)wal->buffer + wal->buffer_used;

    uint64_t header = om_wal_pack_header(seq, type, (uint16_t)data_size);
    memcpy(buf, &header, WAL_HEADER_SIZE);
    wal->buffer_used += WAL_HEADER_SIZE;

    memcpy((char *)wal->buffer + wal->buffer_used, data, data_size);
    wal->buffer_used += data_size;

    if (wal->config.enable_crc32) {
        uint32_t crc = crc32_compute(buf, WAL_HEADER_SIZE + data_size);
        memcpy((char *)wal->buffer + wal->buffer_used, &crc, WAL_CRC32_SIZE);
        wal->buffer_used += WAL_CRC32_SIZE;
    }

    if (wal->post_write) {
        wal->post_write(seq, (uint8_t)type, data, (uint16_t)data_size,
                        wal->post_write_ctx);
    }

    return seq;
}

uint64_t om_wal_insert(OmWal *wal, struct OmSlabSlot *slot, uint16_t product_id) {
    if (!wal || !slot) {
        return 0;
    }

    if (wal->last_error) return 0;
    size_t user_data_size = wal->config.user_data_size;
    size_t aux_data_size = wal->config.aux_data_size;
    size_t crc_size = wal->config.enable_crc32 ? WAL_CRC32_SIZE : 0;
    size_t data_size = sizeof(OmWalInsert) + user_data_size + aux_data_size;
    size_t total_size = WAL_HEADER_SIZE + data_size + crc_size;
    if (data_size > UINT16_MAX || total_size > wal->buffer_size || wal->sequence >= (UINT64_C(1) << 40)) {
        wal->last_error = OM_ERR_WAL_WRITE;
        return 0;
    }

    if (wal->buffer_used + total_size > wal->buffer_size) {
        if (om_wal_flush(wal) != 0) {
            return 0;
        }
    }

    uint64_t seq = wal->sequence++;
    char *record_start = (char *)wal->buffer + wal->buffer_used;

    uint64_t header = om_wal_pack_header(seq, OM_WAL_INSERT, (uint16_t)data_size);
    memcpy(record_start, &header, WAL_HEADER_SIZE);
    wal->buffer_used += WAL_HEADER_SIZE;

    OmWalInsert insert;
    memset(&insert, 0, sizeof(insert));
    
    insert.order_id = slot->order_id;
    insert.price = slot->price;
    insert.volume = slot->volume;
    insert.vol_remain = slot->volume_remain;
    insert.org = slot->org;
    insert.flags = slot->flags;
    insert.product_id = product_id;
    insert.user_data_size = (uint32_t)user_data_size;
    insert.aux_data_size = (uint32_t)aux_data_size;
    insert.timestamp_ns = wal_get_timestamp_ns();

    memcpy((char *)wal->buffer + wal->buffer_used, &insert, sizeof(OmWalInsert));
    wal->buffer_used += sizeof(OmWalInsert);

    if (user_data_size > 0) {
        void *user_data = om_slot_get_data(slot);
        memcpy((char *)wal->buffer + wal->buffer_used, user_data, user_data_size);
        wal->buffer_used += user_data_size;
    }

    if (aux_data_size > 0) {
        if (wal->slab) {
            void *aux_data = om_slot_get_aux_data(wal->slab, slot);
            memcpy((char *)wal->buffer + wal->buffer_used, aux_data, aux_data_size);
        } else {
            memset((char *)wal->buffer + wal->buffer_used, 0, aux_data_size);
        }
        wal->buffer_used += aux_data_size;
    }

    if (wal->config.enable_crc32) {
        uint32_t crc = crc32_compute(record_start, WAL_HEADER_SIZE + data_size);
        memcpy((char *)wal->buffer + wal->buffer_used, &crc, WAL_CRC32_SIZE);
        wal->buffer_used += WAL_CRC32_SIZE;
    }

    if (wal->post_write) {
        wal->post_write(seq, OM_WAL_INSERT, record_start + WAL_HEADER_SIZE,
                        (uint16_t)data_size, wal->post_write_ctx);
    }

    return seq;
}

uint64_t om_wal_cancel(OmWal *wal, uint32_t order_id, uint32_t slot_idx, uint16_t product_id) {
    if (!wal) {
        return 0;
    }

    OmWalCancel rec;
    memset(&rec, 0, sizeof(rec));
    rec.order_id = order_id;
    rec.timestamp_ns = wal_get_timestamp_ns();
    rec.slot_idx = slot_idx;
    rec.product_id = product_id;

    return wal_append(wal, OM_WAL_CANCEL, &rec, sizeof(OmWalCancel));
}

uint64_t om_wal_deactivate(OmWal *wal, uint32_t order_id, uint32_t slot_idx, uint16_t product_id) {
    if (!wal) {
        return 0;
    }

    OmWalDeactivate rec;
    memset(&rec, 0, sizeof(rec));
    rec.order_id = order_id;
    rec.timestamp_ns = wal_get_timestamp_ns();
    rec.slot_idx = slot_idx;
    rec.product_id = product_id;

    return wal_append(wal, OM_WAL_DEACTIVATE, &rec, sizeof(OmWalDeactivate));
}

uint64_t om_wal_activate(OmWal *wal, uint32_t order_id, uint32_t slot_idx, uint16_t product_id) {
    if (!wal) {
        return 0;
    }

    OmWalActivate rec;
    memset(&rec, 0, sizeof(rec));
    rec.order_id = order_id;
    rec.timestamp_ns = wal_get_timestamp_ns();
    rec.slot_idx = slot_idx;
    rec.product_id = product_id;

    return wal_append(wal, OM_WAL_ACTIVATE, &rec, sizeof(OmWalActivate));
}

uint64_t om_wal_match(OmWal *wal, const OmWalMatch *rec) {
    if (!wal || !rec) {
        return 0;
    }
    return wal_append(wal, OM_WAL_MATCH, rec, sizeof(OmWalMatch));
}

/* Write buffer to disk - this is the only syscall in hot path */
int om_wal_flush(OmWal *wal) {
    if (!wal) return OM_ERR_NULL_PARAM;
    if (wal->last_error) return wal->last_error;
    if (wal->buffer_used == 0) {
        return 0;
    }

    /* Align write size to 4KB for O_DIRECT */
    size_t write_size = (wal->buffer_used + WAL_ALIGN - 1) & ~WAL_ALIGN_MASK;
    
    /* Zero-pad to alignment boundary */
    if (write_size > wal->buffer_used) {
        memset((char *)wal->buffer + wal->buffer_used, 0, 
               write_size - wal->buffer_used);
    }

    /* Expand to next WAL file if needed */
    if (wal->config.filename_pattern && wal->config.wal_max_file_size > 0) {
        if (wal->file_offset + write_size > wal->config.wal_max_file_size) {
            if (fsync(wal->fd) != 0) return wal->last_error = OM_ERR_WAL_FSYNC;
            close(wal->fd);
            wal->file_index++;
            if (wal_open_indexed(wal, wal->file_index) != 0) {
                return wal->last_error = OM_ERR_WAL_OPEN;
            }
            wal->file_offset = 0;
        }
    }

    /* Write to file */
    ssize_t written = write(wal->fd, wal->buffer, write_size);
    if (written != (ssize_t)write_size) {
        return wal->last_error = OM_ERR_WAL_WRITE;
    }

    wal->file_offset += write_size;
    wal->buffer_used = 0;

    return 0;
}

/* Force fsync for durability */
int om_wal_fsync(OmWal *wal) {
    if (!wal) return OM_ERR_NULL_PARAM;
    if (wal->last_error) return wal->last_error;
    if (wal->buffer_used > 0) {
        if (om_wal_flush(wal) != 0) {
            return OM_ERR_WAL_FLUSH;
        }
    }

    if (fsync(wal->fd) != 0) {
        return wal->last_error = OM_ERR_WAL_FSYNC;
    }

    return OM_OK;
}

/* ============================================================================
 * WAL REPLAY / RECOVERY IMPLEMENTATION
 * ============================================================================ */

#define REPLAY_BUFFER_SIZE (1024 * 1024)  /* 1MB read buffer */
#define REPLAY_ALIGN 4096

static int wal_replay_open_indexed(OmWalReplay *replay, const char *pattern, uint32_t index) {
    if (!pattern) {
        return OM_ERR_NULL_PARAM;
    }
    char path[512];
    snprintf(path, sizeof(path), pattern, index);
    replay->fd = open(path, O_RDONLY);
    if (replay->fd < 0) {
        return OM_ERR_WAL_OPEN;
    }

    struct stat st;
    if (fstat(replay->fd, &st) != 0) {
        close(replay->fd);
        replay->fd = -1;
        return OM_ERR_WAL_OPEN;
    }
    replay->file_size = st.st_size;
    replay->file_offset = 0;
    replay->file_index = index;
    return 0;
}

int om_wal_replay_init(OmWalReplay *replay, const char *filename) {
    if (!replay || !filename) {
        return OM_ERR_NULL_PARAM;
    }

    memset(replay, 0, sizeof(OmWalReplay));

    /* Open file for reading (without O_DIRECT for simplicity) */
    replay->fd = open(filename, O_RDONLY);
    if (replay->fd < 0) {
        return OM_ERR_WAL_OPEN;
    }

    /* Get file size */
    struct stat st;
    if (fstat(replay->fd, &st) != 0) {
        close(replay->fd);
        replay->fd = -1;
        return OM_ERR_WAL_OPEN;
    }
    replay->file_size = st.st_size;

    /* Allocate aligned buffer */
    replay->buffer_unaligned = malloc(REPLAY_BUFFER_SIZE + REPLAY_ALIGN);
    if (!replay->buffer_unaligned) {
        close(replay->fd);
        replay->fd = -1;
        return OM_ERR_WAL_BUFFER_ALLOC;
    }
    replay->buffer = align_up(replay->buffer_unaligned, REPLAY_ALIGN);
    replay->buffer_size = REPLAY_BUFFER_SIZE;
    replay->buffer_valid = 0;
    replay->buffer_pos = 0;
    replay->file_offset = 0;
    replay->last_sequence = 0;
    replay->eof = false;
    replay->filename_pattern = NULL;
    replay->enable_crc32 = true;  /* CRC on by default */

    return 0;
}

int om_wal_replay_init_with_sizes(OmWalReplay *replay, const char *filename, 
                                   size_t user_data_size, size_t aux_data_size) {
    int ret = om_wal_replay_init(replay, filename);
    if (ret == 0) {
        replay->user_data_size = user_data_size;
        replay->aux_data_size = aux_data_size;
    }
    return ret;
}

int om_wal_replay_init_with_config(OmWalReplay *replay, const char *filename,
                                    const OmWalConfig *config) {
    if (!config) {
        return OM_ERR_NULL_PARAM;
    }
    int ret = 0;
    if (config->filename_pattern) {
        memset(replay, 0, sizeof(OmWalReplay));
        replay->file_index = config->file_index;
        ret = wal_replay_open_indexed(replay, config->filename_pattern, replay->file_index);
        if (ret != 0) {
            return OM_ERR_WAL_OPEN;
        }

        replay->buffer_unaligned = malloc(REPLAY_BUFFER_SIZE + REPLAY_ALIGN);
        if (!replay->buffer_unaligned) {
            close(replay->fd);
            replay->fd = -1;
            return OM_ERR_WAL_BUFFER_ALLOC;
        }
        replay->buffer = align_up(replay->buffer_unaligned, REPLAY_ALIGN);
        replay->buffer_size = REPLAY_BUFFER_SIZE;
        replay->buffer_valid = 0;
        replay->buffer_pos = 0;
        replay->last_sequence = 0;
        replay->eof = false;
        replay->filename_pattern = config->filename_pattern;
    } else {
        ret = om_wal_replay_init(replay, filename);
        if (ret != 0) {
            return ret;
        }
        replay->filename_pattern = NULL;
    }

    replay->user_data_size = config->user_data_size;
    replay->aux_data_size = config->aux_data_size;
    replay->enable_crc32 = !config->disable_crc32;
    return 0;
}

void om_wal_replay_set_user_handler(OmWalReplay *replay,
                                    int (*handler)(OmWalType type, const void *data, size_t len, void *user_ctx),
                                    void *user_ctx) {
    if (!replay) {
        return;
    }
    replay->user_handler = handler;
    replay->user_ctx = user_ctx;
}

void om_wal_replay_close(OmWalReplay *replay) {
    if (!replay) return;

    if (replay->fd >= 0) {
        close(replay->fd);
        replay->fd = -1;
    }

    if (replay->buffer_unaligned) {
        free(replay->buffer_unaligned);
        replay->buffer_unaligned = NULL;
        replay->buffer = NULL;
    }

    replay->buffer_valid = 0;
    replay->buffer_pos = 0;
}

static int replay_advance_file(OmWalReplay *replay);

/* Fill buffer from file */
static int replay_fill_buffer(OmWalReplay *replay) {
    if (replay->file_offset >= replay->file_size) return 0;

    /* Move remaining data to beginning of buffer */
    size_t remaining = replay->buffer_valid - replay->buffer_pos;
    if (remaining > 0) {
        memmove(replay->buffer, 
                (char *)replay->buffer + replay->buffer_pos, 
                remaining);
    }

    /* Read more data */
    size_t to_read = replay->buffer_size - remaining;
    size_t space_left = replay->file_size - replay->file_offset;
    if (to_read > space_left) {
        to_read = space_left;
    }

    ssize_t n = read(replay->fd, (char *)replay->buffer + remaining, to_read);
    if (n < 0) {
        return OM_ERR_WAL_READ;
    }
    if (n == 0) {
        return OM_ERR_WAL_READ;
    }

    replay->buffer_valid = remaining + n;
    replay->buffer_pos = 0;
    replay->file_offset += n;

    return 1;
}

static int replay_advance_file(OmWalReplay *replay) {
    if (!replay || replay->fd < 0) {
        return OM_ERR_NULL_PARAM;
    }
    if (!replay->filename_pattern) {
        return OM_ERR_NULL_PARAM;
    }
    close(replay->fd);
    replay->fd = -1;
    replay->file_index++;
    if (wal_replay_open_indexed(replay, replay->filename_pattern, replay->file_index) != 0) {
        replay->eof = true;
        return errno == ENOENT ? 0 : OM_ERR_WAL_READ;
    }
    replay->buffer_valid = 0;
    replay->buffer_pos = 0;
    replay->eof = false;
    return 1;
}

int om_wal_replay_next(OmWalReplay *r, OmWalType *type, void **data,
                       uint64_t *sequence, size_t *data_len) {
    if (!r || !type || !data || !sequence || !data_len) return OM_ERR_NULL_PARAM;
    const size_t crc_size = r->enable_crc32 ? WAL_CRC32_SIZE : 0;
    for (;;) {
        size_t available = r->buffer_valid - r->buffer_pos;
        if (available == 0 && r->file_offset >= r->file_size) {
            if (!r->filename_pattern) return 0;
            int rc = replay_advance_file(r);
            if (rc <= 0) return rc;
        }
        while (r->buffer_valid - r->buffer_pos < sizeof(OmWalHeader) && r->file_offset < r->file_size) {
            int rc = replay_fill_buffer(r);
            if (rc <= 0) return OM_ERR_WAL_READ;
        }
        available = r->buffer_valid - r->buffer_pos;
        if (!available) continue;
        uint64_t offset = r->file_offset - r->buffer_valid + r->buffer_pos;
        unsigned char *start = (unsigned char *)r->buffer + r->buffer_pos;
        uint64_t packed = 0;
        if (available >= sizeof(packed)) memcpy(&packed, start, sizeof(packed));
        /* Padding is ONLY all-zero bytes through the next writer block boundary.
         * Short headers can be padding too; nonzero damage never means EOF. */
        if (available < sizeof(packed) || packed == 0) {
            size_t pad = WAL_ALIGN - (offset & WAL_ALIGN_MASK);
            while (r->buffer_valid - r->buffer_pos < pad && r->file_offset < r->file_size) {
                int rc = replay_fill_buffer(r);
                if (rc <= 0) return OM_ERR_WAL_READ;
            }
            if (r->buffer_valid - r->buffer_pos < pad) return OM_ERR_WAL_TRUNCATED;
            start = (unsigned char *)r->buffer + r->buffer_pos;
            for (size_t i = 0; i < pad; i++) if (start[i]) return OM_ERR_WAL_READ;
            r->buffer_pos += pad;
            continue;
        }
        uint8_t tag = om_wal_header_type(packed);
        size_t len = om_wal_header_len(packed);
        uint64_t seq = om_wal_header_seq(packed);
        if (tag < OM_WAL_INSERT || (tag > OM_WAL_ACTIVATE && tag < OM_WAL_USER_BASE) ||
            !seq || (r->last_sequence && seq != r->last_sequence + 1)) return OM_ERR_WAL_READ;
        size_t total = sizeof(packed) + len + crc_size;
        if (total > r->buffer_size) return OM_ERR_WAL_TRUNCATED;
        while (r->buffer_valid - r->buffer_pos < total && r->file_offset < r->file_size) {
            int rc = replay_fill_buffer(r);
            if (rc <= 0) return OM_ERR_WAL_READ;
        }
        if (r->buffer_valid - r->buffer_pos < total) return OM_ERR_WAL_TRUNCATED;
        start = (unsigned char *)r->buffer + r->buffer_pos;
        if (tag == OM_WAL_INSERT) {
            OmWalInsert rec;
            if (len < sizeof(rec)) return OM_ERR_WAL_READ;
            memcpy(&rec, start + sizeof(packed), sizeof(rec));
            if ((uint64_t)sizeof(rec) + rec.user_data_size + rec.aux_data_size != len)
                return OM_ERR_WAL_READ;
        } else if ((tag == OM_WAL_CANCEL && len != sizeof(OmWalCancel)) ||
                   (tag == OM_WAL_MATCH && len != sizeof(OmWalMatch)) ||
                   (tag == OM_WAL_DEACTIVATE && len != sizeof(OmWalDeactivate)) ||
                   (tag == OM_WAL_ACTIVATE && len != sizeof(OmWalActivate))) return OM_ERR_WAL_READ;
        r->last_record_offset = offset;
        if (r->enable_crc32) {
            memcpy(&r->last_stored_crc, start + sizeof(packed) + len, WAL_CRC32_SIZE);
            r->last_computed_crc = crc32_compute(start, sizeof(packed) + len);
            if (r->last_stored_crc != r->last_computed_crc) return OM_ERR_WAL_CRC_MISMATCH;
        }
        *type = (OmWalType)tag; *sequence = seq; *data_len = len;
        *data = start + sizeof(packed);
        r->buffer_pos += total;
        r->last_sequence = seq;
        if (tag >= OM_WAL_USER_BASE && r->user_handler && r->user_handler(*type, *data, len, r->user_ctx))
            return OM_ERR_WAL_READ;
        return 1;
    }
}

uint64_t om_wal_append_custom(OmWal *wal, OmWalType type, const void *data, size_t len) {
    if (!wal || !data) {
        return 0;
    }
    if (type < OM_WAL_USER_BASE) {
        return 0;
    }
    return wal_append(wal, type, data, len);
}

/* Helper to extract user data from INSERT record during replay */
void *om_wal_insert_get_user_data(OmWalInsert *insert, size_t user_data_size) {
    if (!insert || user_data_size == 0) return NULL;
    return (char *)insert + sizeof(OmWalInsert);
}

/* Helper to extract aux data from INSERT record during replay */
void *om_wal_insert_get_aux_data(OmWalInsert *insert, size_t user_data_size, size_t aux_data_size) {
    if (!insert || aux_data_size == 0) return NULL;
    return (char *)insert + sizeof(OmWalInsert) + user_data_size;
}
