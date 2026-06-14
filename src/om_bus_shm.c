#include "ombus/om_bus.h"

#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* ============================================================================
 * Internal helpers
 * ============================================================================ */

static inline void _om_bus_cpu_relax(void) {
#if defined(__x86_64__) || defined(__i386__)
    __asm__ volatile("pause" ::: "memory");
#elif defined(__aarch64__)
    __asm__ volatile("yield" ::: "memory");
#endif
}

static inline bool _om_bus_is_power_of_two(uint32_t v) {
    return v != 0 && (v & (v - 1U)) == 0U;
}

static inline uint64_t _om_bus_monotonic_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

#ifdef OM_BUS_ENABLE_ERRNO_FALLBACK
static bool _om_bus_ftruncate_fallback_ok(int err, size_t current_size, size_t required_size) {
    if (current_size < required_size) {
        return false;
    }
    return err == EBUSY || err == EINVAL || err == EACCES || err == EPERM;
}
#endif

/* CRC32C (Castagnoli polynomial: 0x82F63B78)
 * Hardware-accelerated on x86 (SSE4.2) and ARM (CRC extension).
 * Falls back to software table otherwise. */

#if defined(__x86_64__) && defined(__SSE4_2__)
#include <nmmintrin.h>
#define OM_BUS_CRC32C_HW 1
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#define OM_BUS_CRC32C_HW 2
#endif

#ifdef OM_BUS_CRC32C_HW

static uint32_t _om_bus_crc32(const void *data, size_t len) {
    const uint8_t *buf = (const uint8_t *)data;
    uint32_t crc = 0xFFFFFFFF;

#if OM_BUS_CRC32C_HW == 1
    /* x86 SSE4.2 — process 8 bytes at a time */
    while (len >= 8) {
        uint64_t val;
        memcpy(&val, buf, 8);
        crc = (uint32_t)_mm_crc32_u64(crc, val);
        buf += 8;
        len -= 8;
    }
    while (len > 0) {
        crc = _mm_crc32_u8(crc, *buf++);
        len--;
    }
#elif OM_BUS_CRC32C_HW == 2
    /* ARM CRC extension — process 8 bytes at a time */
    while (len >= 8) {
        uint64_t val;
        memcpy(&val, buf, 8);
        crc = __crc32cd(crc, val);
        buf += 8;
        len -= 8;
    }
    while (len > 0) {
        crc = __crc32cb(crc, *buf++);
        len--;
    }
#endif

    return crc ^ 0xFFFFFFFF;
}

#else /* Software fallback */

static uint32_t _om_bus_crc32c_table[256];
static _Atomic bool _om_bus_crc32_ready = false;

static void _om_bus_crc32_init(void) {
    if (atomic_load_explicit(&_om_bus_crc32_ready, memory_order_acquire)) return;
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++) {
            c = (c >> 1) ^ ((c & 1) ? 0x82F63B78U : 0U);
        }
        _om_bus_crc32c_table[i] = c;
    }
    atomic_store_explicit(&_om_bus_crc32_ready, true, memory_order_release);
}

static uint32_t _om_bus_crc32(const void *data, size_t len) {
    _om_bus_crc32_init();
    const uint8_t *buf = (const uint8_t *)data;
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        crc = _om_bus_crc32c_table[(crc ^ buf[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

#endif /* OM_BUS_CRC32C_HW */

/* Compute total SHM file size */
static size_t _om_bus_shm_size(uint32_t capacity, uint32_t slot_size,
                                uint32_t max_consumers) {
    return OM_BUS_HEADER_PAGE
         + (size_t)max_consumers * OM_BUS_CONSUMER_ALIGN
         + (size_t)capacity * slot_size;
}

/* Pointer to consumer tail array (after header page) */
static inline OmBusConsumerTail *_om_bus_consumer_tails(void *base) {
    return (OmBusConsumerTail *)((char *)base + OM_BUS_HEADER_PAGE);
}

/* Pointer to slot i (after header page + consumer tails) */
static inline void *_om_bus_slot(void *base, uint32_t max_consumers,
                                  uint32_t slot_size, uint64_t idx) {
    char *slots_base = (char *)base + OM_BUS_HEADER_PAGE
                     + (size_t)max_consumers * OM_BUS_CONSUMER_ALIGN;
    return slots_base + idx * slot_size;
}

/* Like _om_bus_min_tail but skips consumers whose last_poll_ns is stale */
static uint64_t _om_bus_min_tail_live(OmBusConsumerTail *tails, uint32_t count,
                                       uint64_t staleness_ns, uint64_t fallback) {
    uint64_t min_val = UINT64_MAX;
    uint64_t now = staleness_ns ? _om_bus_monotonic_ns() : 0;
    for (uint32_t i = 0; i < count; i++) {
        uint64_t t = atomic_load_explicit(&tails[i].tail, memory_order_acquire);
        uint64_t poll_ns = atomic_load_explicit(&tails[i].last_poll_ns,
                                                 memory_order_relaxed);
        if (poll_ns == 0) continue;
        if (staleness_ns) {
            if ((now - poll_ns) > staleness_ns) continue;
        }
        if (t < min_val) min_val = t;
    }
    return min_val == UINT64_MAX ? fallback : min_val;
}

static inline bool _om_bus_is_broadcast(uint32_t flags) {
    return (flags & OM_BUS_FLAG_BROADCAST) != 0U;
}

/* ============================================================================
 * OmBusStream — producer
 * ============================================================================ */

struct OmBusStream {
    void *map;                  /* mmap base pointer */
    size_t map_size;            /* mmap region length */
    OmBusShmHeader *hdr;        /* == map */
    OmBusConsumerTail *tails;   /* consumer tail array */
    uint32_t slot_size;
    uint32_t capacity;
    uint32_t mask;
    uint32_t max_consumers;
    uint32_t flags;
    char shm_name[64];         /* for shm_unlink on destroy */
    uint64_t records_published; /* stats counter */
    uint64_t staleness_ns;     /* consumer staleness threshold (0 = disabled) */
    OmBusBackpressureCb backpressure_cb;
    void *backpressure_ctx;
};

static void _om_bus_publish_slot(OmBusStream *stream, uint64_t pos,
                                  const OmBusRecord *rec) {
    uint64_t idx = pos & stream->mask;
    OmBusSlotHeader *slot = (OmBusSlotHeader *)_om_bus_slot(
        stream->map, stream->max_consumers, stream->slot_size, idx);

    if (_om_bus_is_broadcast(stream->flags)) {
        atomic_store_explicit(&slot->seq, 0U, memory_order_relaxed);
        atomic_thread_fence(memory_order_release);
    }

    char *payload_dst = (char *)slot + OM_BUS_SLOT_HEADER_SIZE;
    if (rec->payload && rec->payload_len > 0) {
        memcpy(payload_dst, rec->payload, rec->payload_len);
    }

    slot->wal_seq = rec->wal_seq;
    slot->wal_type = rec->wal_type;
    slot->reserved = 0;
    slot->payload_len = rec->payload_len;
    slot->crc32 = (stream->flags & OM_BUS_FLAG_CRC)
        ? _om_bus_crc32(rec->payload, rec->payload_len) : 0;

    atomic_store_explicit(&slot->seq, pos + 1U, memory_order_release);
}

int om_bus_stream_create(OmBusStream **out, const OmBusStreamConfig *config) {
    if (!out || !config || !config->stream_name) {
        return OM_ERR_BUS_INIT;
    }

    uint32_t capacity = config->capacity ? config->capacity : OM_BUS_DEFAULT_CAPACITY;
    uint32_t slot_size = config->slot_size ? config->slot_size : OM_BUS_DEFAULT_SLOT_SIZE;
    uint32_t max_consumers = config->max_consumers ? config->max_consumers : OM_BUS_DEFAULT_MAX_CONSUMERS;

    if (!_om_bus_is_power_of_two(capacity)) {
        return OM_ERR_BUS_NOT_POW2;
    }
    if (slot_size < OM_BUS_SLOT_HEADER_SIZE + 1) {
        return OM_ERR_BUS_INIT;
    }

    size_t total = _om_bus_shm_size(capacity, slot_size, max_consumers);

    int fd = shm_open(config->stream_name, O_CREAT | O_RDWR, 0600);
    if (fd < 0) {
        return OM_ERR_BUS_SHM_CREATE;
    }

    struct stat st;
    bool needs_resize = true;
    if (fstat(fd, &st) == 0) {
        needs_resize = ((size_t)st.st_size < total);
    }

    if (needs_resize && ftruncate(fd, (off_t)total) != 0) {
#ifdef OM_BUS_ENABLE_ERRNO_FALLBACK
        int trunc_errno = errno;
        struct stat post_trunc_st;
        bool have_post_size = (fstat(fd, &post_trunc_st) == 0);
        size_t post_size = have_post_size ? (size_t)post_trunc_st.st_size : 0U;
        if (!_om_bus_ftruncate_fallback_ok(trunc_errno, post_size, total)) {
            close(fd);
            shm_unlink(config->stream_name);
            return OM_ERR_BUS_SHM_CREATE;
        }
#else
        close(fd);
        shm_unlink(config->stream_name);
        return OM_ERR_BUS_SHM_CREATE;
#endif
    }

    void *map = mmap(NULL, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (map == MAP_FAILED) {
        shm_unlink(config->stream_name);
        return OM_ERR_BUS_SHM_MAP;
    }

    /* Zero-fill (mmap of ftruncate'd file is already zeroed, but be explicit) */
    memset(map, 0, total);

    /* Initialize header */
    OmBusShmHeader *hdr = (OmBusShmHeader *)map;
    hdr->magic = OM_BUS_SHM_MAGIC;
    hdr->version = OM_BUS_SHM_VERSION;
    hdr->slot_size = slot_size;
    hdr->capacity = capacity;
    hdr->max_consumers = max_consumers;
    hdr->flags = config->flags;
    atomic_init(&hdr->head, 0U);
    atomic_init(&hdr->min_tail, 0U);
    atomic_init(&hdr->producer_epoch, _om_bus_monotonic_ns());
    hdr->staleness_ns = config->staleness_ns;
    strncpy(hdr->stream_name, config->stream_name, sizeof(hdr->stream_name) - 1);
    hdr->stream_name[sizeof(hdr->stream_name) - 1] = '\0';

    /* Initialize consumer tails */
    OmBusConsumerTail *tails = _om_bus_consumer_tails(map);
    for (uint32_t i = 0; i < max_consumers; i++) {
        atomic_init(&tails[i].tail, 0U);
        atomic_init(&tails[i].wal_seq, 0U);
        atomic_init(&tails[i].last_poll_ns, 0U);
    }

    /* Initialize slot sequences */
    for (uint32_t i = 0; i < capacity; i++) {
        OmBusSlotHeader *slot = (OmBusSlotHeader *)_om_bus_slot(
            map, max_consumers, slot_size, i);
        atomic_init(&slot->seq, (uint64_t)i);
    }

    /* Allocate stream handle */
    OmBusStream *s = calloc(1, sizeof(*s));
    if (!s) {
        munmap(map, total);
        shm_unlink(config->stream_name);
        return OM_ERR_BUS_INIT;
    }
    s->map = map;
    s->map_size = total;
    s->hdr = hdr;
    s->tails = tails;
    s->slot_size = slot_size;
    s->capacity = capacity;
    s->mask = capacity - 1U;
    s->max_consumers = max_consumers;
    s->flags = config->flags;
    s->staleness_ns = config->staleness_ns;
    s->backpressure_cb = config->backpressure_cb;
    s->backpressure_ctx = config->backpressure_ctx;
    strncpy(s->shm_name, config->stream_name, sizeof(s->shm_name) - 1);
    s->shm_name[sizeof(s->shm_name) - 1] = '\0';

    *out = s;
    return 0;
}

int om_bus_stream_publish(OmBusStream *stream, uint64_t wal_seq,
                          uint8_t wal_type, const void *payload, uint16_t len) {
    if (!stream) return OM_ERR_BUS_INIT;
    if (len > stream->slot_size - OM_BUS_SLOT_HEADER_SIZE) {
        return OM_ERR_BUS_RECORD_TOO_LARGE;
    }

    uint64_t head = atomic_load_explicit(&stream->hdr->head, memory_order_relaxed);

    /* Backpressure: phased spin while ring is full
     * Phase 1: 10 iterations  — cpu_relax()      (~100ns)
     * Phase 2: 32 iterations  — cpu_relax()      (~300ns)
     * Phase 3: sched_yield()  + callback          (~50-100us) */
    if (!_om_bus_is_broadcast(stream->flags)) {
        uint32_t pressure_spins = 0;
        while (1) {
            uint64_t mt = atomic_load_explicit(&stream->hdr->min_tail, memory_order_acquire);
            if ((head - mt) < stream->capacity) break;
            if ((pressure_spins & 31U) == 0U) {
                mt = _om_bus_min_tail_live(stream->tails, stream->max_consumers,
                                            stream->staleness_ns, head);
                atomic_store_explicit(&stream->hdr->min_tail, mt, memory_order_release);
            }
            if (pressure_spins < 10) {
                _om_bus_cpu_relax();
            } else if (pressure_spins < 42) {
                _om_bus_cpu_relax();
            } else {
                if (pressure_spins == 42 && stream->backpressure_cb) {
                    stream->backpressure_cb(head, mt, stream->backpressure_ctx);
                }
                sched_yield();
            }
            pressure_spins++;
        }
    }

    OmBusRecord rec = {
        .wal_seq = wal_seq,
        .wal_type = wal_type,
        .payload_len = len,
        .payload = payload,
    };
    _om_bus_publish_slot(stream, head, &rec);
    atomic_store_explicit(&stream->hdr->head, head + 1U, memory_order_release);
    stream->records_published++;

    return 0;
}

int om_bus_stream_publish_batch(OmBusStream *stream, const OmBusRecord *recs,
                                 uint32_t count) {
    if (!stream || (!recs && count > 0)) return OM_ERR_BUS_INIT;

    /* Validate all records fit before writing any */
    uint32_t max_payload = stream->slot_size - OM_BUS_SLOT_HEADER_SIZE;
    for (uint32_t i = 0; i < count; i++) {
        if (recs[i].payload_len > max_payload) return OM_ERR_BUS_RECORD_TOO_LARGE;
    }

    uint64_t head = atomic_load_explicit(&stream->hdr->head, memory_order_relaxed);
    uint32_t i = 0;

    while (i < count) {
        uint64_t mt = 0;
        uint32_t pressure_spins = 0;
        if (!_om_bus_is_broadcast(stream->flags)) {
            while (1) {
                mt = atomic_load_explicit(&stream->hdr->min_tail, memory_order_acquire);
                if ((head - mt) < stream->capacity) break;
                if ((pressure_spins & 31U) == 0U) {
                    mt = _om_bus_min_tail_live(stream->tails, stream->max_consumers,
                                                stream->staleness_ns, head);
                    atomic_store_explicit(&stream->hdr->min_tail, mt,
                                          memory_order_release);
                }
                if (pressure_spins < 10) {
                    _om_bus_cpu_relax();
                } else if (pressure_spins < 42) {
                    _om_bus_cpu_relax();
                } else {
                    if (pressure_spins == 42 && stream->backpressure_cb) {
                        stream->backpressure_cb(head, mt, stream->backpressure_ctx);
                    }
                    sched_yield();
                }
                pressure_spins++;
            }
        }

        uint64_t available = _om_bus_is_broadcast(stream->flags)
            ? (uint64_t)(count - i) : stream->capacity - (head - mt);
        uint32_t chunk = count - i;
        if ((uint64_t)chunk > available) {
            chunk = (uint32_t)available;
        }

        for (uint32_t j = 0; j < chunk; j++) {
            _om_bus_publish_slot(stream, head, &recs[i + j]);
            head++;
        }

        i += chunk;
    }

    /* Single head advancement for the batch */
    atomic_store_explicit(&stream->hdr->head, head, memory_order_release);
    stream->records_published += count;

    return 0;
}

void om_bus_stream_stats(const OmBusStream *s, OmBusStreamStats *out) {
    if (!s || !out) return;
    out->records_published = s->records_published;
    out->head = atomic_load_explicit(&s->hdr->head, memory_order_relaxed);
    out->min_tail = atomic_load_explicit(&s->hdr->min_tail, memory_order_relaxed);
}

void om_bus_stream_destroy(OmBusStream *stream) {
    if (!stream) return;
    if (stream->map && stream->map != MAP_FAILED) {
        munmap(stream->map, stream->map_size);
    }
    shm_unlink(stream->shm_name);
    free(stream);
}

/* ============================================================================
 * OmBusEndpoint — consumer
 * ============================================================================ */

struct OmBusEndpoint {
    void *map;                  /* mmap base pointer */
    size_t map_size;            /* mmap region length */
    OmBusShmHeader *hdr;
    OmBusConsumerTail *tails;
    uint32_t consumer_index;
    uint32_t slot_size;
    uint32_t capacity;
    uint32_t mask;
    uint32_t max_consumers;
    uint32_t flags;
    bool zero_copy;
    uint64_t expected_wal_seq;  /* For gap detection */
    uint64_t producer_epoch;    /* Epoch at time of open, for restart detection */
    void *copy_buf;             /* Copy buffer (when !zero_copy) */
};

int om_bus_endpoint_open(OmBusEndpoint **out, const OmBusEndpointConfig *config) {
    if (!out || !config || !config->stream_name) {
        return OM_ERR_BUS_INIT;
    }

    int fd = shm_open(config->stream_name, O_RDWR, 0);
    if (fd < 0) {
        return OM_ERR_BUS_SHM_OPEN;
    }

    /* Read header page first to learn layout */
    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < (off_t)OM_BUS_HEADER_PAGE) {
        close(fd);
        return OM_ERR_BUS_SHM_OPEN;
    }

    size_t total = (size_t)st.st_size;
    void *map = mmap(NULL, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (map == MAP_FAILED) {
        return OM_ERR_BUS_SHM_MAP;
    }

    OmBusShmHeader *hdr = (OmBusShmHeader *)map;

    /* Validate magic & version */
    if (hdr->magic != OM_BUS_SHM_MAGIC) {
        munmap(map, total);
        return OM_ERR_BUS_MAGIC_MISMATCH;
    }
    if (hdr->version != OM_BUS_SHM_VERSION) {
        munmap(map, total);
        return OM_ERR_BUS_VERSION_MISMATCH;
    }
    if (config->consumer_index >= hdr->max_consumers) {
        munmap(map, total);
        return OM_ERR_BUS_CONSUMER_ID;
    }
    if ((hdr->flags & OM_BUS_FLAG_BROADCAST) && config->zero_copy) {
        munmap(map, total);
        return OM_ERR_BUS_INIT;
    }

    OmBusEndpoint *ep = calloc(1, sizeof(*ep));
    if (!ep) {
        munmap(map, total);
        return OM_ERR_BUS_INIT;
    }

    ep->map = map;
    ep->map_size = total;
    ep->hdr = hdr;
    ep->tails = _om_bus_consumer_tails(map);
    ep->consumer_index = config->consumer_index;
    ep->slot_size = hdr->slot_size;
    ep->capacity = hdr->capacity;
    ep->mask = hdr->capacity - 1U;
    ep->max_consumers = hdr->max_consumers;
    ep->flags = hdr->flags;
    ep->zero_copy = config->zero_copy;
    ep->expected_wal_seq = 0;
    ep->producer_epoch = atomic_load_explicit(&hdr->producer_epoch,
                                               memory_order_acquire);

    if (!config->zero_copy) {
        ep->copy_buf = malloc(hdr->slot_size);
        if (!ep->copy_buf) {
            munmap(map, total);
            free(ep);
            return OM_ERR_BUS_INIT;
        }
    }

    /* Initialize consumer tail to current head (start from live position) */
    uint64_t cur_head = atomic_load_explicit(&hdr->head, memory_order_acquire);
    atomic_store_explicit(&ep->tails[config->consumer_index].tail,
                          cur_head, memory_order_release);
    atomic_store_explicit(&ep->tails[config->consumer_index].wal_seq,
                          0U, memory_order_release);
    atomic_store_explicit(&ep->tails[config->consumer_index].last_poll_ns,
                          _om_bus_monotonic_ns(), memory_order_relaxed);

    *out = ep;
    return 0;
}

static int _om_bus_gap_result(OmBusEndpoint *ep, uint64_t wal_seq) {
    int result = 1;
    if (ep->expected_wal_seq > 0 && wal_seq != ep->expected_wal_seq) {
        if (wal_seq > ep->expected_wal_seq) {
            result = OM_ERR_BUS_GAP_DETECTED;
        } else if (ep->flags & OM_BUS_FLAG_REJECT_REORDER) {
            result = OM_ERR_BUS_REORDER_DETECTED;
        }
    }
    ep->expected_wal_seq = wal_seq + 1U;
    return result;
}

static void _om_bus_endpoint_advance(OmBusEndpoint *ep, uint64_t old_tail,
                                      uint64_t new_tail, uint64_t wal_seq) {
    atomic_store_explicit(&ep->tails[ep->consumer_index].tail,
                          new_tail, memory_order_release);
    atomic_store_explicit(&ep->tails[ep->consumer_index].wal_seq,
                          wal_seq, memory_order_release);
    atomic_store_explicit(&ep->tails[ep->consumer_index].last_poll_ns,
                          _om_bus_monotonic_ns(), memory_order_relaxed);

    if (_om_bus_is_broadcast(ep->flags)) return;

    uint64_t cached_min = atomic_load_explicit(&ep->hdr->min_tail, memory_order_acquire);
    if (old_tail == cached_min || new_tail < cached_min) {
        uint64_t mt = _om_bus_min_tail_live(ep->tails, ep->max_consumers,
                                            ep->hdr->staleness_ns, new_tail);
        atomic_store_explicit(&ep->hdr->min_tail, mt, memory_order_release);
    }
}

int om_bus_endpoint_poll(OmBusEndpoint *ep, OmBusRecord *rec) {
    if (!ep || !rec) return OM_ERR_BUS_INIT;

    /* Epoch check: detect producer restart */
    uint64_t epoch = atomic_load_explicit(&ep->hdr->producer_epoch,
                                           memory_order_acquire);
    if (epoch != ep->producer_epoch) return OM_ERR_BUS_EPOCH_CHANGED;

    uint64_t tail = atomic_load_explicit(
        &ep->tails[ep->consumer_index].tail, memory_order_relaxed);
    uint64_t idx = tail & ep->mask;

    OmBusSlotHeader *slot = (OmBusSlotHeader *)_om_bus_slot(
        ep->map, ep->max_consumers, ep->slot_size, idx);

    uint64_t want_seq = tail + 1U;
    uint64_t seq = atomic_load_explicit(&slot->seq, memory_order_acquire);
    if (_om_bus_is_broadcast(ep->flags)) {
        if (seq == 0U || seq < want_seq) return 0;
        if (seq > want_seq) return OM_ERR_BUS_LAPPED;
    } else if (seq != want_seq) {
        return 0;
    }

    /* Read header fields */
    uint64_t wal_seq = slot->wal_seq;
    uint8_t wal_type = slot->wal_type;
    uint16_t payload_len = slot->payload_len;
    uint32_t crc32 = slot->crc32;

    const void *payload_src = (const char *)slot + OM_BUS_SLOT_HEADER_SIZE;

    /* Deliver payload */
    if (ep->zero_copy) {
        rec->payload = payload_src;
    } else {
        memcpy(ep->copy_buf, payload_src, payload_len);
        rec->payload = ep->copy_buf;
    }

    if (_om_bus_is_broadcast(ep->flags)) {
        atomic_thread_fence(memory_order_acquire);
        if (atomic_load_explicit(&slot->seq, memory_order_acquire) != seq) {
            return OM_ERR_BUS_LAPPED;
        }
    }

    if (ep->flags & OM_BUS_FLAG_CRC) {
        uint32_t computed = _om_bus_crc32(rec->payload, payload_len);
        if (computed != crc32) return OM_ERR_BUS_CRC_MISMATCH;
    }

    rec->wal_seq = wal_seq;
    rec->wal_type = wal_type;
    rec->payload_len = payload_len;

    int result = _om_bus_gap_result(ep, rec->wal_seq);

    /* Advance tail */
    _om_bus_endpoint_advance(ep, tail, tail + 1U, rec->wal_seq);

    return result;
}

int om_bus_endpoint_poll_batch(OmBusEndpoint *ep, OmBusRecord *recs,
                               size_t max_count) {
    if (!ep || !recs) return OM_ERR_BUS_INIT;
    if (max_count == 0) return 0;
    if (_om_bus_is_broadcast(ep->flags) && max_count > 1) max_count = 1;

    /* Epoch check */
    uint64_t epoch = atomic_load_explicit(&ep->hdr->producer_epoch,
                                           memory_order_acquire);
    if (epoch != ep->producer_epoch) return OM_ERR_BUS_EPOCH_CHANGED;

    uint64_t tail = atomic_load_explicit(
        &ep->tails[ep->consumer_index].tail, memory_order_relaxed);
    size_t count = 0;

    while (count < max_count) {
        uint64_t idx = (tail + count) & ep->mask;

        OmBusSlotHeader *slot = (OmBusSlotHeader *)_om_bus_slot(
            ep->map, ep->max_consumers, ep->slot_size, idx);

        uint64_t want_seq = tail + count + 1U;
        uint64_t seq = atomic_load_explicit(&slot->seq, memory_order_acquire);
        if (_om_bus_is_broadcast(ep->flags)) {
            if (seq == 0U || seq < want_seq) break;
            if (seq > want_seq) {
                if (count == 0) return OM_ERR_BUS_LAPPED;
                break;
            }
        } else if (seq != want_seq) {
            break;
        }

        uint64_t wal_seq = slot->wal_seq;
        uint8_t wal_type = slot->wal_type;
        uint16_t payload_len = slot->payload_len;
        uint32_t crc32 = slot->crc32;

        const void *payload_src = (const char *)slot + OM_BUS_SLOT_HEADER_SIZE;

        if (_om_bus_is_broadcast(ep->flags)) {
            memcpy(ep->copy_buf, payload_src, payload_len);
            atomic_thread_fence(memory_order_acquire);
            if (atomic_load_explicit(&slot->seq, memory_order_acquire) != seq) {
                if (count == 0) return OM_ERR_BUS_LAPPED;
                break;
            }
            if (ep->flags & OM_BUS_FLAG_CRC) {
                uint32_t computed = _om_bus_crc32(ep->copy_buf, payload_len);
                if (computed != crc32) break;
            }
            recs[count].payload = ep->copy_buf;
        } else {
            if (ep->flags & OM_BUS_FLAG_CRC) {
                uint32_t computed = _om_bus_crc32(payload_src, payload_len);
                if (computed != crc32) break;
            }
            if (ep->zero_copy) {
                recs[count].payload = payload_src;
            } else {
                /* For non-broadcast batch, preserve existing zero-copy-like semantics. */
                recs[count].payload = payload_src;
            }
        }

        recs[count].wal_seq = wal_seq;
        recs[count].wal_type = wal_type;
        recs[count].payload_len = payload_len;
        count++;
    }

    if (count > 0) {
        uint64_t new_tail = tail + count;
        ep->expected_wal_seq = recs[count - 1].wal_seq + 1U;
        _om_bus_endpoint_advance(ep, tail, new_tail, recs[count - 1].wal_seq);
    }

    return (int)count;
}

int om_bus_endpoint_seek_head(OmBusEndpoint *ep) {
    if (!ep) return OM_ERR_BUS_INIT;

    uint64_t head = atomic_load_explicit(&ep->hdr->head, memory_order_acquire);
    atomic_store_explicit(&ep->tails[ep->consumer_index].tail,
                          head, memory_order_release);
    atomic_store_explicit(&ep->tails[ep->consumer_index].last_poll_ns,
                          _om_bus_monotonic_ns(), memory_order_relaxed);
    return 0;
}

uint64_t om_bus_endpoint_wal_seq(const OmBusEndpoint *ep) {
    if (!ep) return 0;
    return atomic_load_explicit(&ep->tails[ep->consumer_index].wal_seq,
                                memory_order_acquire);
}

void om_bus_endpoint_close(OmBusEndpoint *ep) {
    if (!ep) return;
    atomic_store_explicit(&ep->tails[ep->consumer_index].last_poll_ns,
                          0U, memory_order_release);
    free(ep->copy_buf);
    if (ep->map && ep->map != MAP_FAILED) {
        munmap(ep->map, ep->map_size);
    }
    free(ep);
}

/* ============================================================================
 * Consumer Cursor Persistence
 * ============================================================================ */

int om_bus_endpoint_save_cursor(const OmBusEndpoint *ep, const char *path) {
    if (!ep || !path) return OM_ERR_BUS_INIT;

    uint64_t wal_seq = atomic_load_explicit(&ep->tails[ep->consumer_index].wal_seq,
                                             memory_order_relaxed);

    uint8_t buf[16];
    uint32_t magic = OM_BUS_CURSOR_MAGIC;
    memcpy(buf, &magic, 4);
    memcpy(buf + 4, &wal_seq, 8);
    uint32_t crc = _om_bus_crc32(&wal_seq, sizeof(wal_seq));
    memcpy(buf + 12, &crc, 4);

    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return OM_ERR_BUS_SHM_OPEN;

    ssize_t n = write(fd, buf, 16);
    close(fd);
    return (n == 16) ? 0 : OM_ERR_BUS_SHM_OPEN;
}

int om_bus_endpoint_load_cursor(const char *path, uint64_t *wal_seq_out) {
    if (!path || !wal_seq_out) return OM_ERR_BUS_INIT;

    int fd = open(path, O_RDONLY);
    if (fd < 0) return OM_ERR_BUS_SHM_OPEN;

    uint8_t buf[16];
    ssize_t n = read(fd, buf, 16);
    close(fd);
    if (n != 16) return OM_ERR_BUS_SHM_OPEN;

    uint32_t magic;
    memcpy(&magic, buf, 4);
    if (magic != OM_BUS_CURSOR_MAGIC) return OM_ERR_BUS_CRC_MISMATCH;

    uint64_t wal_seq;
    memcpy(&wal_seq, buf + 4, 8);

    uint32_t stored_crc;
    memcpy(&stored_crc, buf + 12, 4);
    uint32_t computed_crc = _om_bus_crc32(&wal_seq, sizeof(wal_seq));
    if (stored_crc != computed_crc) return OM_ERR_BUS_CRC_MISMATCH;

    *wal_seq_out = wal_seq;
    return 0;
}
