#ifndef OM_MARKET_H
#define OM_MARKET_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <khash.h>
#include "openmatch/om_wal.h"
#include "openmatch/om_slab.h"

/**
 * @file om_market.h
 * @brief OpenMarket public API
 *
 * Uses slab + intrusive queue architecture:
 * - Fixed-size 32-byte price level slots in contiguous slab
 * - Q0: free list for slab allocation
 * - Q1: sorted price ladder (bids descending, asks ascending)
 * - uint32_t indices instead of pointers (cache-friendly)
 * - Hash map for O(1) price → slot lookup
 */

#define OM_MARKET_VERSION_MAJOR 1U
#define OM_MARKET_VERSION_MINOR 0U
#define OM_MARKET_VERSION_PATCH 0U

#define OM_MARKET_SLOT_NULL UINT32_MAX

typedef struct OmMarketVersion {
    uint32_t major;
    uint32_t minor;
    uint32_t patch;
} OmMarketVersion;

typedef struct OmMarketSubscription {
    uint16_t org_id;
    uint16_t product_id;
} OmMarketSubscription;

typedef struct OmMarketOrderState {
    uint16_t product_id;
    uint16_t side;
    bool active;
    uint16_t org;
    uint16_t flags;
    uint64_t price;
    uint64_t remaining;
    uint64_t vol_remain;    /**< original vol_remain at insert */
} OmMarketOrderState;

KHASH_MAP_INIT_INT64(om_market_order_map, OmMarketOrderState)
KHASH_SET_INIT_INT64(om_market_order_set)
KHASH_MAP_INIT_INT(om_market_pair_map, uint32_t)
KHASH_MAP_INIT_INT64(om_market_delta_map, int64_t)
KHASH_MAP_INIT_INT64(om_market_level_map, uint32_t)  /**< price → slot_idx */
KHASH_MAP_INIT_INT64(om_market_qty_map, uint64_t)    /**< price → qty (per-org) */

/**
 * Price level slot - exactly 32 bytes (2 slots per 64-byte cache line).
 *
 * Each slot is either in Q0 (free list) or Q1 (price ladder), never both.
 * Links could be unioned but kept separate for future expansion.
 */
typedef struct OmMarketLevelSlot {
    /* Q0: free list links (8 bytes) */
    uint32_t q0_next;           /**< next free slot */
    uint32_t q0_prev;           /**< prev free slot */

    /* Q1: price ladder links (8 bytes) */
    uint32_t q1_next;           /**< next slot (worse price) */
    uint32_t q1_prev;           /**< prev slot (better price) */

    /* Data (16 bytes) */
    uint64_t price;
    uint64_t qty;
} OmMarketLevelSlot;            /* 32 bytes exactly */

/**
 * Slab allocator for price level slots.
 * Q0 is the free list - slots are allocated from head, freed to head.
 */
typedef struct OmMarketLevelSlab {
    OmMarketLevelSlot *slots;   /**< contiguous array (cache-line aligned allocation) */
    uint32_t capacity;          /**< total slots */
    uint32_t q0_head;           /**< free list head */
    uint32_t q0_tail;           /**< free list tail */
    uint32_t free_count;        /**< slots available */
} OmMarketLevelSlab;

typedef struct OmMarketLevel {
    uint64_t price;
    uint64_t qty;
} OmMarketLevel;

/**
 * Price ladder using intrusive Q1 queue.
 * Bids: sorted descending (head = best/highest price)
 * Asks: sorted ascending (head = best/lowest price)
 */
typedef struct OmMarketLadder {
    /* Q1 heads/tails for bid side */
    uint32_t bid_head;          /**< best bid (highest price) */
    uint32_t bid_tail;          /**< worst bid (lowest price) */
    uint32_t bid_count;         /**< active bid levels */
    uint32_t bid_hint;          /**< recent bid insertion hint */

    /* Q1 heads/tails for ask side */
    uint32_t ask_head;          /**< best ask (lowest price) */
    uint32_t ask_tail;          /**< worst ask (highest price) */
    uint32_t ask_count;         /**< active ask levels */
    uint32_t ask_hint;          /**< recent ask insertion hint */

    /* O(1) price lookup */
    khash_t(om_market_level_map) *price_to_slot;
} OmMarketLadder;

typedef struct OmMarketDelta {
    uint64_t price;
    int64_t delta;
} OmMarketDelta;


typedef uint64_t (*OmMarketDealableFn)(const OmWalInsert *rec, uint16_t viewer_org, void *ctx);

/**
 * Private worker - sharded by org.
 * Each worker owns its own slab (no cross-worker sharing).
 */
typedef struct OmMarketWorker {
    uint32_t worker_id;
    uint16_t max_products;
    uint32_t subscription_count;
    uint32_t org_count;
    uint32_t *product_offsets;
    uint16_t *product_orgs;
    uint32_t *product_ladder_indices;
    uint16_t *org_ids;
    uint32_t *org_index_map;
    uint32_t *ladder_index;
    size_t ladder_index_stride;
    uint8_t *product_has_subs;
    uint32_t top_levels;
    OmMarketLevelSlab product_slab;  /**< Worker-owned slab for product-level price slots */
    OmMarketLadder *product_ladders; /**< Per-product ladders (Q1 queue heads) [max_products] */
    khash_t(om_market_order_set) **product_order_sets; /**< Per-product order_id sets [max_products] */
    khash_t(om_market_order_map) *global_orders; /**< order_id -> state for product ladder */
    khash_t(om_market_qty_map) *scratch_qty_map; /**< Reused temp map for copy_full */
    uint8_t *ladder_dirty;          /**< 64-byte aligned dirty flags */
    khash_t(om_market_delta_map) **ladder_deltas;
    khash_t(om_market_pair_map) *pair_to_ladder;
    /* Dirty work-list: the indices of ladders marked dirty since the last
     * compaction, so a flush walks O(dirty) instead of O(subscription_count).
     * `ladder_org`/`ladder_product` are the reverse of the (org,product)->ladder
     * map, letting a flush recover a dirty ladder's keys without a hash probe. */
    uint32_t *dirty_list;           /**< ladder indices pending publish (cap subscription_count) */
    uint32_t dirty_count;           /**< live entries in dirty_list */
    uint16_t *ladder_org;           /**< ladder idx -> org_id */
    uint16_t *ladder_product;       /**< ladder idx -> product_id */
    OmMarketDealableFn dealable;
    void *dealable_ctx;
} OmMarketWorker;

/**
 * Public worker - sharded by product.
 * Each worker owns its own slab (no cross-worker sharing).
 */
typedef struct OmMarketPublicWorker {
    uint16_t max_products;
    uint8_t *product_has_subs;
    uint32_t top_levels;
    OmMarketLevelSlab slab;         /**< Worker-owned slab for price level slots */
    OmMarketLadder *ladders;        /**< Per-product ladders (Q1 queue heads) */
    uint8_t *dirty;                 /**< 64-byte aligned dirty flags */
    uint16_t *dirty_list;           /**< product ids pending publish (cap max_products) */
    uint32_t dirty_count;           /**< live entries in dirty_list */
    khash_t(om_market_delta_map) **deltas;
    khash_t(om_market_order_map) *orders;
} OmMarketPublicWorker;

typedef struct OmMarketConfig {
    uint16_t max_products;
    uint32_t worker_count;
    uint32_t public_worker_count;
    const uint32_t *org_to_worker;        /**< Array indexed by org_id */
    const uint32_t *product_to_public_worker; /**< Array indexed by product_id */
    const OmMarketSubscription *subs;
    uint32_t sub_count;
    size_t expected_orders_per_worker;
    size_t expected_subscribers_per_product; /**< Avg orgs per product */
    size_t expected_price_levels;
    uint32_t top_levels;                      /**< Top N price levels to aggregate */
    OmMarketDealableFn dealable;
    void *dealable_ctx;
} OmMarketConfig;

typedef struct OmMarket {
    OmMarketWorker *workers;
    uint32_t worker_count;
    OmMarketPublicWorker *public_workers;
    uint32_t public_worker_count;
    uint16_t max_products;
    uint32_t top_levels;
    OmMarketDealableFn dealable;
    void *dealable_ctx;
} OmMarket;


/**
 * Get OpenMarket version as a struct.
 * @return Version struct (major, minor, patch)
 */
OmMarketVersion om_market_version(void);

/**
 * Get OpenMarket version string.
 * @return Null-terminated version string
 */
const char *om_market_version_string(void);

/**
 * Initialize market aggregation workers.
 * @param market Market instance
 * @param config Configuration (subscriptions, workers, sizing)
 * @return 0 on success, negative on error
 */
int om_market_init(OmMarket *market, const OmMarketConfig *config);

/**
 * Destroy market aggregation workers.
 * @param market Market instance
 */
void om_market_destroy(OmMarket *market);

/**
 * Get worker by id.
 * @param market Market instance
 * @param worker_id Worker id
 * @return Worker pointer or NULL
 */
OmMarketWorker *om_market_worker(OmMarket *market, uint32_t worker_id);

/**
 * Process a WAL record with a worker.
 * @param worker Worker instance
 * @param type WAL record type
 * @param data WAL record data
 * @return 0 on success, negative on error
 */
int om_market_worker_process(OmMarketWorker *worker, OmWalType type, const void *data);

int om_market_public_process(OmMarketPublicWorker *worker, OmWalType type, const void *data);

/**
 * Get aggregated quantity for a worker's org/product/price ladder.
 * @param worker Worker instance
 * @param org_id Org id
 * @param product_id Product id
 * @param side OM_SIDE_BID/OM_SIDE_ASK
 * @param price Price level
 * @param out_qty Output quantity
 * @return 0 if found, -1 if not found or invalid
 */
int om_market_worker_get_qty(const OmMarketWorker *worker,
                             uint16_t org_id,
                             uint16_t product_id,
                             uint16_t side,
                             uint64_t price,
                             uint64_t *out_qty);

/**
 * Get public aggregated quantity for a product/price ladder.
 * @param worker Worker instance
 * @param product_id Product id
 * @param side OM_SIDE_BID/OM_SIDE_ASK
 * @param price Price level
 * @param out_qty Output quantity
 * @return 0 if found, -1 if not found or invalid
 */
int om_market_public_get_qty(const OmMarketPublicWorker *worker,
                             uint16_t product_id,
                             uint16_t side,
                             uint64_t price,
                             uint64_t *out_qty);

/**
 * Check whether an org subscribes to a product in this worker.
 * @param worker Worker instance
 * @param org_id Org id
 * @param product_id Product id
 * @return 1 if subscribed, 0 if not, negative on error
 */
int om_market_worker_is_subscribed(const OmMarketWorker *worker,
                                  uint16_t org_id,
                                  uint16_t product_id);

/**
 * Get count of private ladder deltas for (org, product, side).
 * @return count on success, 0 if none, negative on error
 */
int om_market_worker_delta_count(const OmMarketWorker *worker,
                                 uint16_t org_id,
                                 uint16_t product_id,
                                 uint16_t side);

/**
 * Copy private ladder deltas into caller buffer.
 * @return number of deltas copied, negative on error
 */
int om_market_worker_copy_deltas(const OmMarketWorker *worker,
                                 uint16_t org_id,
                                 uint16_t product_id,
                                 uint16_t side,
                                 OmMarketDelta *out,
                                 size_t max);

/**
 * Clear private ladder deltas after publish.
 * @return 0 on success, negative on error
 */
int om_market_worker_clear_deltas(OmMarketWorker *worker,
                                  uint16_t org_id,
                                  uint16_t product_id,
                                  uint16_t side);

/**
 * Get count of public ladder deltas for product/side.
 * @return count on success, 0 if none, negative on error
 */
int om_market_public_delta_count(const OmMarketPublicWorker *worker,
                                 uint16_t product_id,
                                 uint16_t side);

/**
 * Copy public ladder deltas into caller buffer.
 * @return number of deltas copied, negative on error
 */
int om_market_public_copy_deltas(const OmMarketPublicWorker *worker,
                                 uint16_t product_id,
                                 uint16_t side,
                                 OmMarketDelta *out,
                                 size_t max);

/**
 * Clear public ladder deltas after publish.
 * @return 0 on success, negative on error
 */
int om_market_public_clear_deltas(OmMarketPublicWorker *worker,
                                  uint16_t product_id,
                                  uint16_t side);

/**
 * Copy full private ladder (top-N) for (org, product, side).
 * @return number of levels copied, negative on error
 */
int om_market_worker_copy_full(const OmMarketWorker *worker,
                               uint16_t org_id,
                               uint16_t product_id,
                               uint16_t side,
                               OmMarketDelta *out,
                               size_t max);

/**
 * Copy full public ladder (top-N) for product/side.
 * @return number of levels copied, negative on error
 */
int om_market_public_copy_full(const OmMarketPublicWorker *worker,
                               uint16_t product_id,
                               uint16_t side,
                               OmMarketDelta *out,
                               size_t max);

/**
 * Check whether a ladder changed since last publish.
 * @param worker Worker instance
 * @param org_id Org id
 * @param product_id Product id
 * @return 1 if dirty, 0 if clean, negative on error
 */
int om_market_worker_is_dirty(const OmMarketWorker *worker,
                              uint16_t org_id,
                              uint16_t product_id);

/**
 * Clear dirty flag after publishing.
 * @param worker Worker instance
 * @param org_id Org id
 * @param product_id Product id
 * @return 0 on success, negative on error
 */
int om_market_worker_clear_dirty(OmMarketWorker *worker,
                                 uint16_t org_id,
                                 uint16_t product_id);

/**
 * Check whether a public ladder changed since last publish.
 * @param worker Worker instance
 * @param product_id Product id
 * @return 1 if dirty, 0 if clean, negative on error
 */
int om_market_public_is_dirty(const OmMarketPublicWorker *worker, uint16_t product_id);
int om_market_public_clear_dirty(OmMarketPublicWorker *worker, uint16_t product_id);

/* ============================================================================
 * Dirty work-list iteration
 *
 * A flush previously had to probe is_dirty for every subscription (O(books))
 * to find the few that changed. These let it walk only the dirty ladders.
 * Usage per flush: compact (drop entries cleared since the last pass), read
 * dirty_count, then dirty_at(i) for each i to recover the (org, product) keys.
 * Entries are appended on the 0->1 dirty transition; clearing a dirty flag does
 * not remove the entry (compaction does), so a stale entry is harmless (its
 * ladder simply yields no deltas).
 * ============================================================================ */

/** Number of dirty private ladders currently listed. */
uint32_t om_market_worker_dirty_count(const OmMarketWorker *worker);

/** Recover the i-th dirty ladder's (org_id, product_id). 0 on success. */
int om_market_worker_dirty_at(const OmMarketWorker *worker, uint32_t i,
                              uint16_t *org_id, uint16_t *product_id);

/** Drop list entries whose dirty flag was cleared. Call once per flush pass. */
void om_market_worker_compact_dirty(OmMarketWorker *worker);

/** Number of dirty public product ladders currently listed. */
uint32_t om_market_public_dirty_count(const OmMarketPublicWorker *worker);

/** Recover the i-th dirty public ladder's product_id. 0 on success. */
int om_market_public_dirty_at(const OmMarketPublicWorker *worker, uint32_t i,
                              uint16_t *product_id);

/** Drop list entries whose dirty flag was cleared. Call once per flush pass. */
void om_market_public_compact_dirty(OmMarketPublicWorker *worker);


#endif
