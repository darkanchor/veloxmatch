# om_bus — WAL Distribution Bus

Design and implementation reference for `om_bus`, a library (`libvmbus`) that
distributes WAL record streams across process and machine boundaries.

**Status**: Implemented (SHM + TCP transports)

For performance planning and optimization roadmap, see
[`docs/perf_message_bus.md`](perf_message_bus.md).

## 1. Overview & Goals

VeloxMatch distributes WAL records via two transports in `libvmbus`:

| Transport | Scope | Latency | Use Case |
|-----------|-------|---------|----------|
| Shared memory (SHM) | Same host, cross-process | ~50-80ns publish, ~30-50ns poll | Local workers in separate processes |
| TCP | Cross-machine | ~10-50us | Remote workers, DR replicas |

### Design Principles

1. **WAL is source of truth.** The bus is a stateless transport layer. Any
   consumer can recover from a WAL replay — the bus never needs to store
   history.
2. **No routing.** Every consumer sees the full stream. Workers filter
   internally (by product, org, record type) as they do today.
3. **Backpressure over loss.** SHM: slow consumer blocks the producer (spin).
   TCP: slow client is disconnected (send buffer overflow).
4. **WAL sequence numbers as universal cursor.** Every bus record carries
   `wal_seq` — consumers detect gaps and can replay the WAL to recover.
5. **Hot path untouched.** The engine publish path (engine → WAL → post_write
   hook → SHM ring) adds a memcpy + atomic store. TCP runs in a separate relay
   process that reads SHM as a consumer.

## 2. Topology

```
                   ┌───────────────────────────────────────────────┐
                   │                  Host A                       │
                   │                                               │
                   │  ┌──────────┐    SHM ring     ┌────────────┐ │
                   │  │ OmEngine │───────────────→│ Worker P0  │ │
                   │  │ (producer)│                 │ (endpoint) │ │
                   │  └──────────┘        │        └────────────┘ │
                   │                      │        ┌────────────┐ │
                   │                      ├──────→│ Worker P1  │ │
                   │                      │        │ (endpoint) │ │
                   │                      │        └────────────┘ │
                   │                      │        ┌────────────┐ │
                   │                      └──────→│ TCP Relay  │ │
                   │                               │ (endpoint) │ │
                   │                               └─────┬──────┘ │
                   └─────────────────────────────────────┼────────┘
                                                         │ TCP
                   ┌─────────────────────────────────────┼────────┐
                   │                  Host B             │        │
                   │                               ┌─────▼──────┐ │
                   │                               │ TCP Client │ │
                   │                               └─────┬──────┘ │
                   │                      ┌──────────────┼───┐    │
                   │                      │              │   │    │
                   │                ┌─────▼────┐  ┌─────▼────┐   │
                   │                │ Worker R0│  │ Worker R1│   │
                   │                └──────────┘  └──────────┘   │
                   └──────────────────────────────────────────────┘
```

**Data flow**: Engine → WAL `post_write` hook → `OmBusStream` (SHM) →
`OmBusEndpoint` (local workers) or → relay process → `OmBusTcpServer` →
`OmBusTcpClient` (remote workers).

The WAL-to-bus connection is made via a header-only glue function
(`om_bus_attach_wal()` in `om_bus_wal.h`) that sets the WAL's `post_write`
callback to `om_bus_stream_publish()`. No link-time dependency between
`libveloxmatch` and `libvmbus`.

## 3. Common Types

Both transports deliver the same `OmBusRecord`:

```c
typedef struct OmBusRecord {
    uint64_t    wal_seq;        /* WAL sequence number */
    uint8_t     wal_type;       /* OmWalType enum value */
    uint16_t    payload_len;    /* Payload byte count */
    const void *payload;        /* Pointer to payload data */
} OmBusRecord;
```

- SHM: `payload` points into the mmap region (zero-copy) or a copy buffer.
- TCP: `payload` points into the client recv buffer, valid until the next
  `om_bus_tcp_client_poll()` call.

## 4. Shared Memory Transport

### 4.1 Ring Slot Format

Fixed-size slots in a memory-mapped POSIX shared memory object. Default slot
size: **256 bytes** (configurable). Each slot contains a 24-byte header
followed by inline payload data — no pointers, valid across address spaces.

```
┌─────────────────────────── 256 bytes (default) ──────────────────────────┐
│                                                                          │
│  ┌─────────────────────── 24 bytes: header ────────────────────────┐     │
│  │ _Atomic uint64_t  seq;        // slot sequence (publish fence)  │     │
│  │ uint64_t          wal_seq;    // WAL sequence number            │     │
│  │ uint8_t           wal_type;   // OmWalType (INSERT, CANCEL...) │     │
│  │ uint8_t           reserved;   // padding                        │     │
│  │ uint16_t          payload_len; // bytes of payload following    │     │
│  │ uint32_t          crc32;      // CRC32 of payload              │     │
│  └─────────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  ┌─────────────────── (slot_size - 24) bytes: payload ─────────────┐    │
│  │ Raw WAL record data (OmWalInsert, OmWalCancel, OmWalMatch...)   │    │
│  │ Copied inline — same binary layout as WAL on-disk records       │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

**Slot header: `OmBusSlotHeader`** (24 bytes):

| Field | Type | Size | Description |
|-------|------|------|-------------|
| `seq` | `_Atomic uint64_t` | 8B | Monotonic slot sequence; publish fence |
| `wal_seq` | `uint64_t` | 8B | WAL sequence from `OmWalHeader` |
| `wal_type` | `uint8_t` | 1B | `OmWalType` enum value |
| `reserved` | `uint8_t` | 1B | Alignment padding |
| `payload_len` | `uint16_t` | 2B | Payload byte count (max `slot_size - 24`) |
| `crc32` | `uint32_t` | 4B | CRC32 of payload bytes (when `OM_BUS_FLAG_CRC` set) |

**Slot sizing**: The default 256B accommodates all current WAL record types:

| WAL Type | Slot Header | Payload | Total |
|----------|-------------|---------|-------|
| INSERT (no user/aux data) | 24B | 56B (`OmWalInsert`) | 80B |
| INSERT (32B user + 64B aux) | 24B | 152B | 176B |
| CANCEL | 24B | 24B | 48B |
| MATCH | 24B | 40B | 64B |
| DEACTIVATE | 24B | 24B | 48B |
| ACTIVATE | 24B | 24B | 48B |

Records exceeding `slot_size - 24` return `OM_ERR_BUS_RECORD_TOO_LARGE`.

### 4.2 Memory Layout

The SHM file is created via `shm_open()` + `ftruncate()` + `mmap()`:

```
Offset 0                                                    Offset N
┌─────────────┬────────────────────────┬────────────────────────────┐
│ Header Page │ Consumer Tails Section │ Ring Slots                 │
│   (4 KB)    │ (max_consumers × 64B) │ (capacity × slot_size)     │
└─────────────┴────────────────────────┴────────────────────────────┘
```

**Header page** (`OmBusShmHeader`, 4096 bytes, page-aligned):

```c
typedef struct OmBusShmHeader {
    uint32_t magic;             // 0x4F4D4253 ("OMBS")
    uint32_t version;           // Protocol version (1)
    uint32_t slot_size;         // Bytes per slot (default 256)
    uint32_t capacity;          // Number of slots (power of two)
    uint32_t max_consumers;     // Maximum consumer count
    uint32_t flags;             // Feature flags (CRC, REJECT_REORDER, etc.)
    _Atomic uint64_t head;      // Producer head position
    _Atomic uint64_t min_tail;  // Cached minimum consumer tail
    _Atomic uint64_t producer_epoch; // Monotonic ns timestamp, set on create
    char stream_name[64];       // Null-terminated stream name
    uint8_t _pad[4096 - 112];  // Pad to full page
} OmBusShmHeader;
```

**Consumer tails** (`OmBusConsumerTail`, 64 bytes each, cache-line aligned):

```c
typedef struct OmBusConsumerTail {
    _Atomic uint64_t tail;      // Consumer read position
    _Atomic uint64_t wal_seq;   // Last WAL sequence consumed
    _Atomic uint64_t last_poll_ns; // clock_gettime(MONOTONIC) at last poll
    uint8_t _pad[40];           // Pad to 64 bytes (cache line)
} OmBusConsumerTail;
```

### 4.3 Publish Path

Single-producer publish (hot path):

```
1. Backpressure:   spin while (head - min_tail) >= capacity
                   Phase 1: 10 iterations with cpu_relax (pause/yield)
                   Phase 2: 32 iterations with cpu_relax
                   Phase 3: sched_yield() + optional backpressure callback
                   Stale consumers skipped if staleness_ns configured
2. Slot index:     idx = head & mask
3. Copy payload:   memcpy(slot[idx].payload, wal_record, len)
4. Fill header:    wal_seq, wal_type, payload_len, crc32
5. Publish fence:  atomic_store(&slot[idx].seq, head + 1, release)
6. Advance head:   atomic_store(&header->head, head + 1, release)
```

Expected latency: **~50-80ns** (memcpy dominates for typical payloads).

### 4.4 Poll Path

Per-consumer poll (non-blocking):

```
1. Slot index:     idx = my_tail & mask
2. Check ready:    atomic_load(&slot[idx].seq, acquire) != my_tail + 1 → empty
3. CRC check:      (optional) compute CRC32, compare with slot.crc32
4. Read payload:   zero-copy (pointer into mmap) or memcpy to copy buffer
5. Gap check:      wal_seq != expected → OM_ERR_BUS_GAP_DETECTED
6. Advance tail:   atomic_store(&consumer_tails[my_idx].tail, my_tail + 1, release)
7. Refresh min:    if prev_tail == cached_min_tail → recompute from all tails
```

Expected latency: **~30-50ns** per record.

**Zero-copy option**: When `zero_copy = true`, `rec->payload` points directly
into the mmap region. The pointer remains valid until the producer wraps around
and overwrites the slot. Safe when `capacity >> consumer_throughput`.

**Batch poll**: `om_bus_endpoint_poll_batch()` reads up to N records in one
call, advancing the tail once at the end. All payload pointers use zero-copy
semantics regardless of the `zero_copy` flag (the single copy buffer cannot
hold multiple records).

### 4.5 Backpressure & Stale Consumer Detection

The producer spins when `head - min_tail >= capacity`. No records are dropped.
Spin phases escalate: Phase 1 (10 iters, `cpu_relax`), Phase 2 (32 iters,
`cpu_relax`), Phase 3 (`sched_yield` + optional `OmBusBackpressureCb`).

**Stale consumer detection**: When `staleness_ns > 0` in `OmBusStreamConfig`,
the backpressure loop uses `_om_bus_min_tail_live()` which skips consumers
whose `last_poll_ns` is older than the threshold. This prevents dead or
crashed consumers from blocking the ring indefinitely.

For the TCP relay consumer, the relay process must drain its SHM tail promptly.
If the relay falls behind (e.g., TCP backlog), the engine blocks — backpressure
propagates to the producer rather than silently losing records.

### 4.6 Consumer Registration

Consumers are pre-assigned at stream creation via `max_consumers`. Each claims
a slot index in `[0, max_consumers)`, coordinated externally. No dynamic
registration — adding a consumer requires recreating the stream.

On `om_bus_endpoint_open()`, the consumer's tail is initialized to the current
head position (starts from live data, not from the beginning of the ring).

### 4.7 SHM API

```c
/* --- Producer (OmBusStream) --- */

typedef void (*OmBusBackpressureCb)(uint64_t head, uint64_t min_tail, void *ctx);

typedef struct OmBusStreamConfig {
    const char *stream_name;    /* SHM name, e.g. "/om-bus-engine-0" */
    uint32_t    capacity;       /* Ring capacity, power of two (default 4096) */
    uint32_t    slot_size;      /* Bytes per slot (default 256) */
    uint32_t    max_consumers;  /* Max consumer count (default 8) */
    uint32_t    flags;          /* OM_BUS_FLAG_CRC, OM_BUS_FLAG_REJECT_REORDER */
    uint64_t    staleness_ns;   /* Consumer staleness threshold (0 = disabled) */
    OmBusBackpressureCb backpressure_cb;  /* Optional: fires on Phase 3 entry */
    void       *backpressure_ctx;
} OmBusStreamConfig;

int  om_bus_stream_create(OmBusStream **out, const OmBusStreamConfig *config);
int  om_bus_stream_publish(OmBusStream *s, uint64_t wal_seq,
         uint8_t wal_type, const void *payload, uint16_t len);
int  om_bus_stream_publish_batch(OmBusStream *s, const OmBusRecord *recs,
         uint32_t count);
void om_bus_stream_stats(const OmBusStream *s, OmBusStreamStats *out);
void om_bus_stream_destroy(OmBusStream *s);

typedef struct OmBusStreamStats {
    uint64_t records_published;
    uint64_t head;
    uint64_t min_tail;
} OmBusStreamStats;

/* --- Consumer (OmBusEndpoint) --- */

typedef struct OmBusEndpointConfig {
    const char *stream_name;    /* SHM name to attach to */
    uint32_t    consumer_index; /* Pre-assigned consumer index */
    bool        zero_copy;      /* true = payload points into mmap */
} OmBusEndpointConfig;

int      om_bus_endpoint_open(OmBusEndpoint **out, const OmBusEndpointConfig *cfg);
int      om_bus_endpoint_poll(OmBusEndpoint *ep, OmBusRecord *rec);
int      om_bus_endpoint_poll_batch(OmBusEndpoint *ep, OmBusRecord *recs, size_t max);
uint64_t om_bus_endpoint_wal_seq(const OmBusEndpoint *ep);
void     om_bus_endpoint_close(OmBusEndpoint *ep);

/* --- Cursor Persistence --- */
int  om_bus_endpoint_save_cursor(const OmBusEndpoint *ep, const char *path);
int  om_bus_endpoint_load_cursor(const char *path, uint64_t *wal_seq_out);
```

**Feature flags**:
- `OM_BUS_FLAG_CRC` (0x1) — enable CRC32C validation on publish/poll
- `OM_BUS_FLAG_REJECT_REORDER` (0x2) — return error on wal_seq backward

**Return values for poll**:
- `1` — record available in `rec`
- `0` — no record (empty ring)
- `OM_ERR_BUS_GAP_DETECTED` — wal_seq gap found (`rec` still populated)
- `OM_ERR_BUS_CRC_MISMATCH` — CRC check failed
- `OM_ERR_BUS_EPOCH_CHANGED` — producer restarted (epoch mismatch)
- `OM_ERR_BUS_REORDER_DETECTED` — wal_seq backward (when `REJECT_REORDER` set)

## 5. TCP Transport

### 5.1 Architecture

TCP distribution uses two components:

- **`OmBusTcpServer`** — binds a TCP port, accepts connections, broadcasts
  frames to all connected clients. Typically runs inside a relay process that
  reads from an `OmBusEndpoint`.
- **`OmBusTcpClient`** — connects to a server, polls frames into
  `OmBusRecord`. Feeds records to `OmMarketWorker` via the
  `om_bus_tcp_poll_worker()` helper.

The protocol is deliberately simple: no handshake, no subscription, no
batching, no heartbeat. TCP checksumming is sufficient (no application-level
CRC). Clients connect, receive frames, detect gaps via wal_seq tracking.

### 5.2 Wire Protocol

16-byte packed header + payload per frame:

```
Offset  Size  Field         Description
0       4     magic         0x4F4D5446 ("OMTF")
4       1     wal_type      OmWalType enum
5       1     flags         Reserved (0)
6       2     payload_len   Payload bytes (LE)
8       8     wal_seq       WAL sequence (LE)
---     ---
16      N     payload       Raw WAL record data
```

```c
#define OM_BUS_TCP_FRAME_MAGIC       0x4F4D5446U  /* "OMTF" */
#define OM_BUS_TCP_FRAME_HEADER_SIZE 16U

typedef struct OmBusTcpFrameHeader {
    uint32_t magic;        /* OM_BUS_TCP_FRAME_MAGIC */
    uint8_t  wal_type;     /* OmWalType enum value */
    uint8_t  flags;        /* Reserved (0) */
    uint16_t payload_len;  /* Payload bytes (LE) */
    uint64_t wal_seq;      /* WAL sequence (LE) */
} __attribute__((packed)) OmBusTcpFrameHeader;
```

No framing beyond this — each TCP write is one or more complete frames
serialized into the send buffer.

### 5.3 Server

The server manages a fixed-size array of client slots:

```c
struct OmBusTcpServer {
    int                  listen_fd;
    struct pollfd       *pollfds;      /* [0]=listen, [1..N]=clients */
    uint32_t            *pfd_to_slot;  /* pollfd index → client slot index */
    OmBusTcpClientSlot  *clients;      /* client slot array */
    uint32_t             max_clients;
    uint32_t             client_count;
    uint32_t             send_buf_size; /* per-client send buffer */
    uint16_t             port;          /* actual bound port */
    /* Stats counters */
    uint64_t             records_broadcast;
    uint64_t             bytes_broadcast;
    uint64_t             clients_accepted;
    uint64_t             clients_disconnected;
    uint64_t             slow_client_drops;
};

typedef struct OmBusTcpClientSlot {
    int      fd;                /* -1 = unused */
    uint8_t *send_buf;          /* linear send buffer */
    uint32_t send_buf_size;
    uint32_t send_used;         /* total bytes (offset + unsent) */
    uint32_t send_offset;       /* bytes already flushed */
    bool     disconnect_pending;
} OmBusTcpClientSlot;
```

**`broadcast()`** serializes the frame header + payload into each client's send
buffer. Does NOT perform I/O — just appends bytes. If a client's buffer
overflows after compaction, it is marked `disconnect_pending`.

**`poll_io()`** drives all I/O in a single non-blocking `poll()` call:

1. Build `pollfd` array: listen_fd (`POLLIN`) + each active client
   (`POLLIN` for disconnect detection, `POLLOUT` if data pending)
2. `poll(pollfds, nfds, 0)` — non-blocking
3. Accept new connections (set non-blocking, `TCP_NODELAY`,
   `SO_NOSIGPIPE` on macOS)
4. For each client: flush send buffer on `POLLOUT`, detect HUP/ERR,
   detect FIN via `recv(..., MSG_PEEK)`
5. Close clients marked `disconnect_pending`

**Typical relay loop**:

```c
while (running) {
    OmBusRecord rec;
    while (om_bus_endpoint_poll(ep, &rec) > 0) {
        om_bus_tcp_server_broadcast(srv, rec.wal_seq,
            rec.wal_type, rec.payload, rec.payload_len);
    }
    om_bus_tcp_server_poll_io(srv);
}
```

### 5.4 Client

```c
struct OmBusTcpClient {
    int      fd;
    uint8_t *recv_buf;
    uint32_t recv_buf_size;
    uint32_t recv_used;         /* total bytes in buffer */
    uint32_t recv_offset;       /* bytes already consumed */
    uint64_t expected_wal_seq;
    uint64_t last_wal_seq;
    uint32_t flags;             /* OM_BUS_FLAG_REJECT_REORDER, etc. */
    bool     peer_closed;       /* deferred disconnect: drain buffer first */
};
```

**`connect()`** performs a blocking TCP connect, then sets the socket
non-blocking (`fcntl(O_NONBLOCK)`, `TCP_NODELAY`, `SO_NOSIGPIPE` on macOS).

**`poll()`** is non-blocking:

1. Compact recv buffer: `memmove` unconsumed data to front (deferred from
   previous call so payload pointer stays valid between calls)
2. `recv()` into buffer (non-blocking)
3. Parse one frame header from `recv_offset`:
   - Magic mismatch → `OM_ERR_BUS_TCP_PROTOCOL`
   - Incomplete header/payload → return `0`
4. Set `rec->payload` to point into recv buffer (valid until next `poll()`)
5. Advance `recv_offset` past the consumed frame
6. Gap detection: if `wal_seq != expected_wal_seq` → `OM_ERR_BUS_GAP_DETECTED`
   (record is still populated)

### 5.5 Backpressure & Slow Client Warning

TCP backpressure is handled per-client on the server:

1. `broadcast()` attempts to append frame to client's send buffer
2. If it doesn't fit, compact buffer first (`memmove` unsent data to front)
3. If still doesn't fit after compaction:
   a. Inject a **slow client warning** frame into the send buffer (if room):
      `wal_type = 0xFE`, payload = `[bytes_pending:4][buf_capacity:4]`
   b. Mark `disconnect_pending = true`
4. `poll_io()` flushes remaining data (including the warning) for
   `disconnect_pending` clients, then closes via `shutdown(SHUT_WR)` +
   `close()` for graceful delivery
5. Client drains buffered frames before reporting `OM_ERR_BUS_TCP_DISCONNECTED`,
   ensuring the warning frame (`OM_ERR_BUS_TCP_SLOW_WARNING`) is delivered

This ensures the relay process never blocks on a slow TCP client. The relay's
SHM endpoint advances promptly, keeping the engine's SHM publish unblocked.

### 5.6 TCP API

```c
/* --- Server (OmBusTcpServer) --- */

typedef struct OmBusTcpServerConfig {
    const char *bind_addr;      /* NULL = "0.0.0.0" */
    uint16_t    port;           /* 0 = ephemeral */
    uint32_t    max_clients;    /* default 64 */
    uint32_t    send_buf_size;  /* per-client, default 256 KB */
} OmBusTcpServerConfig;

int      om_bus_tcp_server_create(OmBusTcpServer **out, const OmBusTcpServerConfig *cfg);
int      om_bus_tcp_server_broadcast(OmBusTcpServer *srv, uint64_t wal_seq,
             uint8_t wal_type, const void *payload, uint16_t len);
int      om_bus_tcp_server_poll_io(OmBusTcpServer *srv);
uint32_t om_bus_tcp_server_client_count(const OmBusTcpServer *srv);
uint16_t om_bus_tcp_server_port(const OmBusTcpServer *srv);
void     om_bus_tcp_server_stats(const OmBusTcpServer *srv, OmBusTcpServerStats *out);
void     om_bus_tcp_server_destroy(OmBusTcpServer *srv);

typedef struct OmBusTcpServerStats {
    uint64_t records_broadcast;
    uint64_t bytes_broadcast;
    uint64_t clients_accepted;
    uint64_t clients_disconnected;
    uint64_t slow_client_drops;
} OmBusTcpServerStats;

/* --- Client (OmBusTcpClient) --- */

typedef struct OmBusTcpClientConfig {
    const char *host;
    uint16_t    port;
    uint32_t    recv_buf_size;  /* default 256 KB */
    uint32_t    flags;          /* OM_BUS_FLAG_REJECT_REORDER, etc. */
} OmBusTcpClientConfig;

int      om_bus_tcp_client_connect(OmBusTcpClient **out, const OmBusTcpClientConfig *cfg);
int      om_bus_tcp_client_poll(OmBusTcpClient *client, OmBusRecord *rec);
uint64_t om_bus_tcp_client_wal_seq(const OmBusTcpClient *client);
void     om_bus_tcp_client_close(OmBusTcpClient *client);

/* --- Auto-Reconnect Client (OmBusTcpAutoClient) --- */

typedef struct OmBusTcpAutoClientConfig {
    OmBusTcpClientConfig base;       /* host, port, recv_buf_size, flags */
    uint32_t max_retries;            /* 0 = unlimited */
    uint32_t retry_base_ms;          /* initial backoff (default 100ms) */
    uint32_t retry_max_ms;           /* max backoff (default 5000ms) */
} OmBusTcpAutoClientConfig;

int      om_bus_tcp_auto_client_create(OmBusTcpAutoClient **out,
             const OmBusTcpAutoClientConfig *cfg);
int      om_bus_tcp_auto_client_poll(OmBusTcpAutoClient *client, OmBusRecord *rec);
uint64_t om_bus_tcp_auto_client_wal_seq(const OmBusTcpAutoClient *client);
void     om_bus_tcp_auto_client_close(OmBusTcpAutoClient *client);
```

**Return values for `client_poll()`**:
- `1` — record available
- `0` — no complete frame yet
- `OM_ERR_BUS_GAP_DETECTED` — wal_seq gap (record still populated)
- `OM_ERR_BUS_TCP_DISCONNECTED` — peer closed or recv error
- `OM_ERR_BUS_TCP_PROTOCOL` — frame magic mismatch
- `OM_ERR_BUS_TCP_SLOW_WARNING` — server warning before disconnect
- `OM_ERR_BUS_REORDER_DETECTED` — wal_seq backward (when `REJECT_REORDER` flag set)

**Return values for `auto_client_poll()`**:
- `1` — record available
- `0` — no frame or reconnecting (backoff in progress)
- Negative — permanent failure (max retries exhausted)

### 5.7 Platform Portability

| Concern | Linux | macOS |
|---------|-------|-------|
| SIGPIPE suppression | `MSG_NOSIGNAL` flag on `send()` | `SO_NOSIGPIPE` socket option |
| I/O multiplexing | `poll()` | `poll()` |
| Non-blocking | `fcntl(O_NONBLOCK)` | `fcntl(O_NONBLOCK)` |
| TCP tuning | `TCP_NODELAY` | `TCP_NODELAY` |
| TCP keep-alive idle | `TCP_KEEPIDLE=30` | `TCP_KEEPALIVE=30` |
| TCP keep-alive interval | `TCP_KEEPINTVL=10, TCP_KEEPCNT=3` | `TCP_KEEPINTVL=10, TCP_KEEPCNT=3` |

The implementation uses `poll()` everywhere (POSIX, works on both platforms,
fine for <100 clients).

## 6. Helper Headers

### 6.1 WAL → Bus Glue (`om_bus_wal.h`)

Header-only. Wires the WAL `post_write` callback to `om_bus_stream_publish()`:

```c
static inline void om_bus_attach_wal(OmWal *wal, OmBusStream *stream) {
    om_wal_set_post_write(wal, _om_bus_wal_cb, stream);
}
```

No link-time dependency between `libveloxmatch` and `libvmbus` — the connection
is made via a function pointer set by application code.

### 6.2 SHM Bus → Market Worker (`om_bus_market.h`)

Header-only. Polls one record from an `OmBusEndpoint` and feeds it to a market
worker:

```c
int om_bus_poll_worker(OmBusEndpoint *ep, OmMarketWorker *w);
int om_bus_poll_public(OmBusEndpoint *ep, OmMarketPublicWorker *w);
```

### 6.3 TCP Bus → Market Worker (`om_bus_tcp_market.h`)

Header-only. Same pattern for TCP transport:

```c
int om_bus_tcp_poll_worker(OmBusTcpClient *client, OmMarketWorker *w);
int om_bus_tcp_poll_public(OmBusTcpClient *client, OmMarketPublicWorker *w);
```

## 7. Error Codes

Range **-800 to -823** in `om_bus_error.h`:

```c
/* SHM errors */
OM_ERR_BUS_INIT             = -800,  /* Bus initialization failed */
OM_ERR_BUS_SHM_CREATE       = -801,  /* shm_open/ftruncate failed */
OM_ERR_BUS_SHM_MAP          = -802,  /* mmap failed */
OM_ERR_BUS_SHM_OPEN         = -803,  /* Consumer shm_open failed */
OM_ERR_BUS_NOT_POW2         = -804,  /* Capacity not power of two */
OM_ERR_BUS_CONSUMER_ID      = -805,  /* Invalid consumer index */
OM_ERR_BUS_RECORD_TOO_LARGE = -806,  /* Payload exceeds slot_size - 24 */
OM_ERR_BUS_MAGIC_MISMATCH   = -807,  /* SHM header magic mismatch */
OM_ERR_BUS_VERSION_MISMATCH = -808,  /* SHM header version mismatch */
OM_ERR_BUS_CRC_MISMATCH     = -809,  /* Payload CRC32 mismatch */
OM_ERR_BUS_GAP_DETECTED     = -810,  /* WAL sequence gap detected */
OM_ERR_BUS_EMPTY            = -811,  /* No record available */

/* TCP errors */
OM_ERR_BUS_TCP_BIND         = -812,  /* TCP bind/listen failed */
OM_ERR_BUS_TCP_CONNECT      = -813,  /* TCP connect failed */
OM_ERR_BUS_TCP_SEND         = -814,  /* TCP send failed */
OM_ERR_BUS_TCP_RECV         = -815,  /* TCP recv failed */
OM_ERR_BUS_TCP_DISCONNECTED = -816,  /* TCP peer disconnected */
OM_ERR_BUS_TCP_PROTOCOL     = -817,  /* TCP frame magic mismatch */
OM_ERR_BUS_TCP_IO           = -818,  /* TCP poll() error */
OM_ERR_BUS_TCP_MAX_CLIENTS  = -819,  /* TCP max clients reached */

/* Resilience errors */
OM_ERR_BUS_EPOCH_CHANGED    = -820,  /* Producer epoch changed (restart) */
OM_ERR_BUS_CONSUMER_STALE   = -821,  /* Consumer heartbeat stale */
OM_ERR_BUS_TCP_SLOW_WARNING = -822,  /* Server warned: slow client */
OM_ERR_BUS_REORDER_DETECTED = -823,  /* WAL sequence went backward */
```

## 8. Resilience

### 8.1 Gap Detection

Both transports track `expected_wal_seq` and return `OM_ERR_BUS_GAP_DETECTED`
when a gap is found. The record is still delivered (caller decides whether to
process it or initiate WAL replay first).

| Scenario | Detection | Recovery |
|----------|-----------|----------|
| Consumer restart | First poll: wal_seq > saved cursor | WAL replay from cursor |
| Producer restart | Sequence reset detected | Full WAL replay |
| TCP disconnect | `OM_ERR_BUS_TCP_DISCONNECTED` | Reconnect, resume from `wal_seq` |
| Slow TCP client | Server disconnects on buffer overflow | Reconnect, WAL replay from last `wal_seq` |

### 8.2 Slow Consumer Handling

- **SHM**: Producer blocks (spin-wait). If a consumer is permanently slow,
  it must be deregistered or its workload reduced.
- **TCP**: Server disconnects the slow client. Client can reconnect and replay
  from its last known `wal_seq`.

## 9. Performance

### 9.1 Targets

| Metric | SHM | TCP |
|--------|-----|-----|
| Publish latency | ~50-80ns | N/A (relay is async) |
| Poll latency | ~30-50ns | ~10-50us (network RTT) |
| Throughput | >5M records/sec | ~500K-1M records/sec |
| Memory per stream | 4KB + consumers×64B + capacity×slot_size | +send/recv buffers |

### 9.2 Memory Budget

Default SHM: 4096 slots × 256B = 1MB ring + 4KB header + 512B tails
(8 consumers × 64B) = **~1.05 MB per stream**.

Default TCP: 256 KB send buffer × max_clients (64) = **~16 MB server-side**.
256 KB recv buffer per client.

## 10. File Layout

```
include/ombus/
    om_bus.h                 # SHM stream + endpoint API
    om_bus_tcp.h             # TCP server + client API + frame header
    om_bus_error.h           # Error codes (-800 to -823)
    om_bus_wal.h             # Header-only: WAL post_write → bus publish
    om_bus_market.h          # Header-only: SHM bus → market worker
    om_bus_tcp_market.h      # Header-only: TCP bus → market worker
    om_bus_relay.h           # Header-only: SHM → TCP relay loop
    om_bus_replay.h          # Header-only: WAL replay gap recovery
src/
    om_bus_shm.c             # SHM stream + endpoint implementation
    om_bus_tcp.c             # TCP server + client implementation
tests/
    test_bus.c               # SHM tests (21), WAL-Bus integration (4), TCP tests (20)
```

Build artifacts: `libvmbus.so` / `libvmbus.a`

Dependencies:
- `librt` (Linux only, for `shm_open`)
- Standard POSIX sockets (no additional libraries)

## 11. Test Coverage

124 tests total across all suites. Bus-specific tests:

**SHM TCase** (21 tests):

| Test | Verifies |
|------|----------|
| `test_bus_create_destroy` | Stream lifecycle |
| `test_bus_publish_poll` | Single publish + poll roundtrip with CRC |
| `test_bus_batch` | Batch publish + poll_batch |
| `test_bus_multi_consumer` | Independent consumer cursors |
| `test_bus_backpressure` | Full ring → drain → publish more |
| `test_bus_gap_detection` | Non-contiguous wal_seq → GAP_DETECTED |
| `test_bus_crc_validation` | Corrupted payload → CRC_MISMATCH |
| `test_bus_record_too_large` | Oversized payload → RECORD_TOO_LARGE |
| `test_bus_magic_mismatch` | Corrupted SHM header → MAGIC_MISMATCH |
| `test_bus_wal_seq_tracking` | endpoint_wal_seq() tracks correctly |
| `test_bus_large_payload_boundary` | Max payload (slot_size - 24) with CRC |
| `test_bus_batch_publish` | Batch publish API + stream stats |
| `test_bus_cursor_persistence` | Save/load cursor, corrupt detection |
| `test_bus_epoch_restart` | Producer epoch change → EPOCH_CHANGED |
| `test_bus_stale_consumer` | Stale consumer skipped in backpressure |
| `test_bus_relay` | SHM → relay → TCP → client roundtrip |
| `test_bus_reorder_detection` | wal_seq backward → REORDER_DETECTED |
| `test_bus_batch_poll_crc` | Batch poll validates CRC per record, stops on corruption |
| `test_bus_multiple_gaps` | Sequential gaps: 1→5→20→100 |
| `test_bus_concurrent_consumers` | Interleaved polling from two consumers |
| `test_bus_ring_wrap` | 256 records through 16-slot ring (16 wraps) |

**WAL-Bus TCase** (4 tests):

| Test | Verifies |
|------|----------|
| `test_bus_wal_attach` | Engine INSERT → WAL → bus → correct OmWalInsert |
| `test_bus_wal_match` | Two crossing orders → MATCH record on bus |
| `test_bus_wal_cancel` | Insert + cancel → INSERT + CANCEL on bus |
| `test_bus_worker_roundtrip` | Engine → bus → market worker → correct qty |

**TCP TCase** (20 tests):

| Test | Verifies |
|------|----------|
| `test_tcp_create_destroy` | Server bind, listen, port, destroy |
| `test_tcp_connect_disconnect` | Client connects, client_count, close, detect disconnect |
| `test_tcp_single_record` | Broadcast 1 record, client polls correct OmBusRecord |
| `test_tcp_batch_broadcast` | Broadcast 100 records, client receives all in order |
| `test_tcp_slow_client` | Small send_buf overflow → client disconnected |
| `test_tcp_gap_detection` | wal_seq 1 then 5 → GAP_DETECTED |
| `test_tcp_multi_client` | 3 clients each independently receive all records |
| `test_tcp_server_destroy_connected` | Destroy server → client poll returns DISCONNECTED |
| `test_tcp_wal_seq_tracking` | client_wal_seq() tracks correctly |
| `test_tcp_protocol_error` | Corrupt frame magic → PROTOCOL error |
| `test_tcp_reconnect_resume` | Disconnect, reconnect, wal_seq continuity |
| `test_tcp_max_clients` | max_clients+1 → extra rejected |
| `test_tcp_server_stats` | records/bytes/accepted/disconnected counters |
| `test_tcp_slow_client_stats` | slow_client_drops counter |
| `test_tcp_auto_reconnect` | Auto-reconnect wrapper with backoff |
| `test_tcp_slow_client_warning` | Warning frame (0xFE) delivered before disconnect |
| `test_tcp_reorder_detection` | wal_seq backward → REORDER_DETECTED |
| `test_tcp_drain_on_disconnect` | Client drains buffered frames before reporting disconnect |
| `test_tcp_auto_reconnect_max_retries` | Permanent failure after max_retries exhausted |
| `test_tcp_server_load` | 16 clients × 500 records, all delivered |

All tests use loopback (`127.0.0.1`) with ephemeral port (`port=0`).

## 12. Roadmap — Improvements & Future Work

### 12.1 Performance Improvements

#### P1: Batch Publish API ✅ Done

`om_bus_stream_publish_batch()` publishes multiple records with a single head
advancement. Per-slot seq fences are still written for consumer visibility, but
head and min_tail refresh are amortized across the batch.

#### P2: Backpressure Spin Optimization ✅ Done

Phased backoff: Phase 1 (10 iters cpu_relax), Phase 2 (32 iters cpu_relax),
Phase 3 (sched_yield). Optional `OmBusBackpressureCb` in `OmBusStreamConfig`
fires once when entering Phase 3, giving the engine visibility into stalls.

#### P3: TCP Client Recv Buffer — Deferred Compaction ✅ Done

`om_bus_tcp_client_poll()` only compacts when `recv_offset > recv_buf_size / 2`
or the buffer is full. This halves `memmove()` calls while keeping frame data
contiguous for parsing.

#### P4: TCP Server Send Buffer — Fast Path ✅ Done

`broadcast()` checks `send_used + frame_size <= send_buf_size` first (fast
path goto) before attempting compaction. Only compacts when there's truly no
room at the tail. This avoids `memmove` on the hot broadcast path.

#### P5: CRC32 Table Init — Thread Safety ✅ Done

`_om_bus_crc32_init()` uses `_Atomic bool` with `memory_order_acquire` /
`memory_order_release` for correct thread-safe initialization.

#### P6: SIMD CRC32C ✅ Done

Hardware-accelerated CRC32C (Castagnoli) on x86 (SSE4.2 `_mm_crc32_u64`) and
ARM (`__crc32cd`). Software table fallback for other architectures. Polynomial
changed from IEEE 0xEDB88320 to Castagnoli 0x82F63B78 to enable HW acceleration.
CMake auto-detects `-msse4.2` / `-march=armv8-a+crc` per-file.

### 12.2 Functional Improvements

#### F1: Reference Relay Process ✅ Done

Header-only `om_bus_relay.h` provides `om_bus_relay_run()` — polls SHM endpoint,
broadcasts to TCP server, adaptive sleep on idle (spin 100x then usleep).
Shutdown via `volatile bool *running` flag. Returns 0 on clean shutdown,
negative on SHM error.

#### F2: WAL Replay Helper for Gap Recovery ✅ Done

Header-only `om_bus_replay.h` provides `om_bus_replay_gap()` and
`om_bus_replay_gap_public()` — wraps WAL replay iterator to feed a
`[from_seq, to_seq)` range into private or public market workers.

#### F3: Consumer Cursor Persistence ✅ Done

`om_bus_endpoint_save_cursor()` / `om_bus_endpoint_load_cursor()` persist WAL
sequence to a 16-byte file `[magic:4][wal_seq:8][crc32:4]` for resume after
restart without full WAL replay.

#### F4: TCP Client Auto-Reconnect ✅ Done

`OmBusTcpAutoClient` wraps `OmBusTcpClient` with transparent reconnection.
Exponential backoff (configurable base/max ms, optional retry limit).
`auto_poll()` returns 0 during backoff, 1 on record, negative on permanent
failure. WAL sequence tracking persists across disconnects.

#### F5: TCP Heartbeat / Keep-Alive ✅ Done

OS-managed TCP keep-alive enabled on all sockets (server accept + client
connect): `SO_KEEPALIVE=1`, `TCP_KEEPIDLE=30`, `TCP_KEEPINTVL=10`,
`TCP_KEEPCNT=3`. macOS uses `TCP_KEEPALIVE` for idle time. No wire protocol
change needed.

#### F6: Server-Side Metrics ✅ Done

TCP server stats via `om_bus_tcp_server_stats()`: records_broadcast,
bytes_broadcast, clients_accepted, clients_disconnected, slow_client_drops.

SHM stream stats via `om_bus_stream_stats()`: records_published, head, min_tail.

### 12.3 Resilience Improvements

#### R1: Producer Restart Detection ✅ Done

`producer_epoch` (monotonic ns timestamp) added to `OmBusShmHeader`. Set on
`stream_create`, stored by consumer on `endpoint_open`. Consumers check epoch
on every `poll`/`poll_batch` — returns `OM_ERR_BUS_EPOCH_CHANGED` if producer
restarted.

#### R2: Stale Consumer Detection ✅ Done

`last_poll_ns` (monotonic) added to `OmBusConsumerTail`, updated on every poll.
`staleness_ns` config field on `OmBusStreamConfig` (default 0 = disabled).
Producer's backpressure loop uses `_om_bus_min_tail_live()` which skips
consumers whose `last_poll_ns` is older than the threshold, preventing dead
consumers from blocking the ring.

#### R3: TCP Slow Client Warning ✅ Done

Before disconnecting a slow client, the server injects a warning frame into
the send buffer (if room):

```
wal_type = 0xFE (reserved: SLOW_CLIENT_WARNING)
payload  = [bytes_pending:4][buf_capacity:4]
```

Client poll returns `OM_ERR_BUS_TCP_SLOW_WARNING` (-822) for this frame type.
The server flushes remaining data (including the warning) before closing the
connection via `shutdown(SHUT_WR)` + `close()`. The client drains buffered
frames before reporting disconnection, ensuring the warning is delivered.

#### R4: Gap Detection — Reject Reordered Records ✅ Done

When `OM_BUS_FLAG_REJECT_REORDER` (0x2) is set in stream flags (SHM) or
client config flags (TCP), records with `wal_seq < expected_wal_seq` return
`OM_ERR_BUS_REORDER_DETECTED` (-823). Default: off (backward compatible).
Record is still populated so the caller can inspect the out-of-order sequence.

### 12.4 Test Coverage Notes

All items from the original missing test coverage list have been implemented
except "TCP frame split across recv" which is not reliably testable on
loopback (kernel coalesces small writes). The deferred-compact recv buffer
logic handles partial frames correctly by design.

### 12.5 Implementation Priority

**Phase 6 — Operational Readiness** (next):

1. ~~F5: TCP keep-alive (`TCP_KEEPALIVE` socket option)~~ ✅
2. ~~P5: CRC32 thread safety (`_Atomic bool`)~~ ✅
3. ~~P3: TCP recv buffer deferred compaction~~ ✅
4. ~~F6: Server/stream stats structs~~ ✅
5. ~~Test: TCP reconnect, max clients, stats, batch publish, cursor, boundary~~ ✅

**Phase 7 — Recovery & Tooling**:

6. ~~F1: Reference relay process~~ ✅
7. ~~F2: WAL replay gap helper~~ ✅
8. ~~F3: Consumer cursor persistence~~ ✅
9. ~~R1: Producer restart detection (epoch)~~ ✅

**Phase 8 — Performance & Polish**:

10. ~~P1: Batch publish API~~ ✅
11. ~~P2: Backpressure exponential backoff~~ ✅
12. ~~P6: Hardware CRC32C~~ ✅
13. ~~F4: TCP auto-reconnect wrapper~~ ✅
14. ~~R2: Stale consumer detection~~ ✅
