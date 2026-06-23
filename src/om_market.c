#include "openmarket/om_market.h"
#include "openmatch/om_error.h"
#include <stdlib.h>
#include <string.h>

/* Cache line size for alignment to prevent false sharing */
#define OM_CACHE_LINE_SIZE 64

static void om_market_worker_destroy(OmMarketWorker *worker);
static void om_market_public_worker_destroy(OmMarketPublicWorker *worker);

/* Allocate cache-line aligned memory, zero-initialized */
static void *om_aligned_calloc(size_t count, size_t size) {
    size_t total = count * size;
    if (total == 0) {
        return NULL;
    }
    /* Round up to cache line boundary */
    size_t aligned_size = (total + OM_CACHE_LINE_SIZE - 1) & ~(OM_CACHE_LINE_SIZE - 1);
    void *ptr = NULL;
#if defined(__APPLE__)
    if (posix_memalign(&ptr, OM_CACHE_LINE_SIZE, aligned_size) != 0) {
        return NULL;
    }
#else
    ptr = aligned_alloc(OM_CACHE_LINE_SIZE, aligned_size);
    if (!ptr) {
        return NULL;
    }
#endif
    memset(ptr, 0, aligned_size);
    return ptr;
}

OmMarketVersion om_market_version(void) {
    OmMarketVersion version = {
        .major = OM_MARKET_VERSION_MAJOR,
        .minor = OM_MARKET_VERSION_MINOR,
        .patch = OM_MARKET_VERSION_PATCH
    };
    return version;
}

const char *om_market_version_string(void) {
    return "1.0.0";
}

static uint32_t om_market_pair_key(uint16_t org_id, uint16_t product_id) {
    return ((uint32_t)org_id << 16) | (uint32_t)product_id;
}

/* ============================================================================
 * Slab Operations (Q0 Free List)
 *
 * The slab allocator manages fixed-size 32-byte slots. Free slots are linked
 * via Q0 (q0_next/q0_prev). Allocation pops from head, free pushes to head.
 * ============================================================================ */

static int om_market_slab_init(OmMarketLevelSlab *slab, uint32_t capacity) {
    if (!slab || capacity == 0) {
        return OM_ERR_INVALID_PARAM;
    }

    /* Allocate cache-line aligned slots */
    slab->slots = om_aligned_calloc(capacity, sizeof(OmMarketLevelSlot));
    if (!slab->slots) {
        return OM_ERR_ALLOC_FAILED;
    }

    slab->capacity = capacity;
    slab->free_count = capacity;

    /* Initialize Q0 free list: all slots linked in order */
    for (uint32_t i = 0; i < capacity; i++) {
        slab->slots[i].q0_prev = (i == 0) ? OM_MARKET_SLOT_NULL : (i - 1);
        slab->slots[i].q0_next = (i == capacity - 1) ? OM_MARKET_SLOT_NULL : (i + 1);
        slab->slots[i].q1_prev = OM_MARKET_SLOT_NULL;
        slab->slots[i].q1_next = OM_MARKET_SLOT_NULL;
    }

    slab->q0_head = 0;
    slab->q0_tail = capacity - 1;

    return 0;
}

static void om_market_slab_destroy(OmMarketLevelSlab *slab) {
    if (!slab) {
        return;
    }
    free(slab->slots);
    memset(slab, 0, sizeof(*slab));
}

/* Allocate a slot from Q0 free list. Returns slot index or OM_MARKET_SLOT_NULL. */
static uint32_t om_market_slab_alloc(OmMarketLevelSlab *slab) {
    if (!slab || slab->free_count == 0) {
        return OM_MARKET_SLOT_NULL;
    }

    /* Pop from Q0 head */
    uint32_t idx = slab->q0_head;
    OmMarketLevelSlot *slot = &slab->slots[idx];

    slab->q0_head = slot->q0_next;
    if (slab->q0_head != OM_MARKET_SLOT_NULL) {
        slab->slots[slab->q0_head].q0_prev = OM_MARKET_SLOT_NULL;
    } else {
        slab->q0_tail = OM_MARKET_SLOT_NULL;
    }

    /* Clear Q0 links and reset slot data */
    slot->q0_next = OM_MARKET_SLOT_NULL;
    slot->q0_prev = OM_MARKET_SLOT_NULL;
    slot->q1_next = OM_MARKET_SLOT_NULL;
    slot->q1_prev = OM_MARKET_SLOT_NULL;
    slot->price = 0;
    slot->qty = 0;

    slab->free_count--;
    return idx;
}

/* Grow slab capacity. Returns 0 on success.
 * Since we use uint32_t indices (not pointers), all existing indices remain valid after realloc.
 */
static int om_market_slab_grow(OmMarketLevelSlab *slab) {
    if (!slab) {
        return OM_ERR_NULL_PARAM;
    }

    /* Double capacity (or use minimum growth) */
    uint32_t old_cap = slab->capacity;
    uint32_t new_cap = old_cap * 2;
    if (new_cap < old_cap + 64) {
        new_cap = old_cap + 64;  /* Minimum growth */
    }

    /* Grow with cache-line aligned allocation - indices remain valid */
    OmMarketLevelSlot *new_slots =
        om_aligned_calloc(new_cap, sizeof(OmMarketLevelSlot));
    if (!new_slots) {
        return OM_ERR_ALLOC_FAILED;
    }
    memcpy(new_slots, slab->slots, (size_t)old_cap * sizeof(OmMarketLevelSlot));
    free(slab->slots);
    slab->slots = new_slots;

    /* Initialize new slots and link into Q0 free list */
    for (uint32_t i = old_cap; i < new_cap; i++) {
        OmMarketLevelSlot *slot = &slab->slots[i];
        slot->q0_prev = (i == old_cap) ? slab->q0_tail : (i - 1);
        slot->q0_next = (i == new_cap - 1) ? OM_MARKET_SLOT_NULL : (i + 1);
        slot->q1_prev = OM_MARKET_SLOT_NULL;
        slot->q1_next = OM_MARKET_SLOT_NULL;
    }

    /* Link new slots to existing Q0 tail */
    if (slab->q0_tail != OM_MARKET_SLOT_NULL) {
        slab->slots[slab->q0_tail].q0_next = old_cap;
    } else {
        /* Q0 was empty */
        slab->q0_head = old_cap;
    }
    slab->q0_tail = new_cap - 1;

    slab->capacity = new_cap;
    slab->free_count += (new_cap - old_cap);

    return 0;
}

/* Free a slot back to Q0 free list (push to head). */
static void om_market_slab_free(OmMarketLevelSlab *slab, uint32_t idx) {
    if (!slab || idx >= slab->capacity) {
        return;
    }

    OmMarketLevelSlot *slot = &slab->slots[idx];

    /* Push to Q0 head */
    slot->q0_prev = OM_MARKET_SLOT_NULL;
    slot->q0_next = slab->q0_head;

    if (slab->q0_head != OM_MARKET_SLOT_NULL) {
        slab->slots[slab->q0_head].q0_prev = idx;
    } else {
        slab->q0_tail = idx;
    }

    slab->q0_head = idx;
    slab->free_count++;
}

/* ============================================================================
 * Ladder Operations (Q1 Price Queue)
 *
 * The ladder maintains sorted price levels via Q1 links (q1_next/q1_prev).
 * - Bids: descending order (head = best/highest price)
 * - Asks: ascending order (head = best/lowest price)
 *
 * Hash map provides O(1) price → slot lookup.
 * ============================================================================ */

static int om_ladder_init(OmMarketLadder *ladder) {
    if (!ladder) {
        return OM_ERR_NULL_PARAM;
    }

    ladder->bid_head = OM_MARKET_SLOT_NULL;
    ladder->bid_tail = OM_MARKET_SLOT_NULL;
    ladder->bid_count = 0;
    ladder->bid_hint = OM_MARKET_SLOT_NULL;

    ladder->ask_head = OM_MARKET_SLOT_NULL;
    ladder->ask_tail = OM_MARKET_SLOT_NULL;
    ladder->ask_count = 0;
    ladder->ask_hint = OM_MARKET_SLOT_NULL;

    ladder->price_to_slot = kh_init(om_market_level_map);
    if (!ladder->price_to_slot) {
        return OM_ERR_HASH_INIT;
    }

    return 0;
}

static void om_ladder_destroy(OmMarketLadder *ladder) {
    if (!ladder) {
        return;
    }
    if (ladder->price_to_slot) {
        kh_destroy(om_market_level_map, ladder->price_to_slot);
        ladder->price_to_slot = NULL;
    }
}

/* Find insertion position in Q1 for a new price.
 * Returns the slot index AFTER which to insert, or OM_MARKET_SLOT_NULL to insert at head.
 * For bids (descending): walk from head until slot.price < new_price.
 * For asks (ascending): walk from head until slot.price > new_price.
 */
static uint32_t om_ladder_find_insert_pos(const OmMarketLevelSlab *slab,
                                            const OmMarketLadder *ladder,
                                            uint64_t price,
                                            bool is_bid) {
    uint32_t head = is_bid ? ladder->bid_head : ladder->ask_head;
    uint32_t tail = is_bid ? ladder->bid_tail : ladder->ask_tail;
    uint32_t hint = is_bid ? ladder->bid_hint : ladder->ask_hint;

    if (head == OM_MARKET_SLOT_NULL) {
        return OM_MARKET_SLOT_NULL;  /* Empty, insert at head */
    }

    uint64_t head_price = slab->slots[head].price;
    bool insert_at_head = is_bid ? (price > head_price) : (price < head_price);
    if (insert_at_head) {
        return OM_MARKET_SLOT_NULL;
    }

    /* Optimization: check if new price is worse than tail (common case for new orders) */
    if (tail != OM_MARKET_SLOT_NULL) {
        uint64_t tail_price = slab->slots[tail].price;
        bool insert_after_tail = is_bid ? (price <= tail_price) : (price >= tail_price);
        if (insert_after_tail) {
            return tail;  /* Insert after tail */
        }
    }

    if (hint != OM_MARKET_SLOT_NULL && hint < slab->capacity) {
        uint64_t hint_price = slab->slots[hint].price;
        if (is_bid) {
            uint32_t prev = hint;
            uint32_t curr = slab->slots[hint].q1_next;
            if (price > hint_price) {
                uint32_t walk = hint;
                while (walk != OM_MARKET_SLOT_NULL && slab->slots[walk].price < price) {
                    walk = slab->slots[walk].q1_prev;
                }
                if (walk == OM_MARKET_SLOT_NULL) {
                    return OM_MARKET_SLOT_NULL;
                }
                prev = walk;
                curr = slab->slots[walk].q1_next;
            }
            while (curr != OM_MARKET_SLOT_NULL && slab->slots[curr].price >= price) {
                prev = curr;
                curr = slab->slots[curr].q1_next;
            }
            return prev;
        }

        uint32_t prev = hint;
        uint32_t curr = slab->slots[hint].q1_next;
        if (price < hint_price) {
            uint32_t walk = hint;
            while (walk != OM_MARKET_SLOT_NULL && slab->slots[walk].price > price) {
                walk = slab->slots[walk].q1_prev;
            }
            if (walk == OM_MARKET_SLOT_NULL) {
                return OM_MARKET_SLOT_NULL;
            }
            prev = walk;
            curr = slab->slots[walk].q1_next;
        }
        while (curr != OM_MARKET_SLOT_NULL && slab->slots[curr].price <= price) {
            prev = curr;
            curr = slab->slots[curr].q1_next;
        }
        return prev;
    }

    /* Walk from head to find position */
    uint32_t prev = OM_MARKET_SLOT_NULL;
    uint32_t curr = head;

    while (curr != OM_MARKET_SLOT_NULL) {
        uint64_t curr_price = slab->slots[curr].price;
        bool found_pos = is_bid ? (curr_price < price) : (curr_price > price);
        if (found_pos) {
            return prev;  /* Insert after prev (or at head if prev is NULL) */
        }
        prev = curr;
        curr = slab->slots[curr].q1_next;
    }

    return prev;  /* Insert after last slot */
}

/* Link a slot into Q1 after the given position.
 * If after_idx is OM_MARKET_SLOT_NULL, insert at head.
 */
static void om_ladder_link_after(OmMarketLevelSlab *slab,
                                  OmMarketLadder *ladder,
                                  uint32_t slot_idx,
                                  uint32_t after_idx,
                                  bool is_bid) {
    OmMarketLevelSlot *slot = &slab->slots[slot_idx];
    uint32_t *head = is_bid ? &ladder->bid_head : &ladder->ask_head;
    uint32_t *tail = is_bid ? &ladder->bid_tail : &ladder->ask_tail;
    uint32_t *count = is_bid ? &ladder->bid_count : &ladder->ask_count;
    uint32_t *hint = is_bid ? &ladder->bid_hint : &ladder->ask_hint;

    if (after_idx == OM_MARKET_SLOT_NULL) {
        /* Insert at head */
        slot->q1_prev = OM_MARKET_SLOT_NULL;
        slot->q1_next = *head;
        if (*head != OM_MARKET_SLOT_NULL) {
            slab->slots[*head].q1_prev = slot_idx;
        } else {
            *tail = slot_idx;
        }
        *head = slot_idx;
    } else {
        /* Insert after after_idx */
        OmMarketLevelSlot *after = &slab->slots[after_idx];
        slot->q1_prev = after_idx;
        slot->q1_next = after->q1_next;

        if (after->q1_next != OM_MARKET_SLOT_NULL) {
            slab->slots[after->q1_next].q1_prev = slot_idx;
        } else {
            *tail = slot_idx;
        }
        after->q1_next = slot_idx;
    }

    (*count)++;
    *hint = slot_idx;
}

/* Unlink a slot from Q1. */
static void om_ladder_unlink(OmMarketLevelSlab *slab,
                              OmMarketLadder *ladder,
                              uint32_t slot_idx,
                              bool is_bid) {
    OmMarketLevelSlot *slot = &slab->slots[slot_idx];
    uint32_t *head = is_bid ? &ladder->bid_head : &ladder->ask_head;
    uint32_t *tail = is_bid ? &ladder->bid_tail : &ladder->ask_tail;
    uint32_t *count = is_bid ? &ladder->bid_count : &ladder->ask_count;
    uint32_t *hint = is_bid ? &ladder->bid_hint : &ladder->ask_hint;
    uint32_t next_hint = slot->q1_prev != OM_MARKET_SLOT_NULL
                             ? slot->q1_prev
                             : slot->q1_next;

    if (slot->q1_prev != OM_MARKET_SLOT_NULL) {
        slab->slots[slot->q1_prev].q1_next = slot->q1_next;
    } else {
        *head = slot->q1_next;
    }

    if (slot->q1_next != OM_MARKET_SLOT_NULL) {
        slab->slots[slot->q1_next].q1_prev = slot->q1_prev;
    } else {
        *tail = slot->q1_prev;
    }

    slot->q1_prev = OM_MARKET_SLOT_NULL;
    slot->q1_next = OM_MARKET_SLOT_NULL;

    (*count)--;
    if (*hint == slot_idx) {
        *hint = next_hint;
    }
    if (*count == 0) {
        *hint = OM_MARKET_SLOT_NULL;
    }
}

/* Add quantity to ladder at price.
 * If price exists, increment qty. Otherwise allocate slot and insert into Q1.
 * Returns 0 on success, negative on error.
 */
static int om_ladder_add_qty(OmMarketLevelSlab *slab,
                              OmMarketLadder *ladder,
                              uint64_t price,
                              uint64_t qty,
                              bool is_bid) {
    if (qty == 0) {
        return 0;
    }

    /* Check if price exists in hash map */
    khiter_t it = kh_get(om_market_level_map, ladder->price_to_slot, price);
    if (it != kh_end(ladder->price_to_slot)) {
        /* Price exists, just add quantity */
        uint32_t slot_idx = kh_val(ladder->price_to_slot, it);
        slab->slots[slot_idx].qty += qty;
        return 0;
    }

    /* Allocate new slot, grow slab if needed */
    uint32_t slot_idx = om_market_slab_alloc(slab);
    if (slot_idx == OM_MARKET_SLOT_NULL) {
        /* Slab full - grow and retry */
        int grow_ret = om_market_slab_grow(slab);
        if (grow_ret != 0) {
            return grow_ret;  /* Growth failed (OOM) */
        }
        slot_idx = om_market_slab_alloc(slab);
        if (slot_idx == OM_MARKET_SLOT_NULL) {
            return OM_ERR_SLAB_FULL;  /* Should not happen after successful grow */
        }
    }

    /* Initialize slot */
    OmMarketLevelSlot *slot = &slab->slots[slot_idx];
    slot->price = price;
    slot->qty = qty;

    /* Add to hash map */
    int ret = 0;
    it = kh_put(om_market_level_map, ladder->price_to_slot, price, &ret);
    if (ret < 0) {
        om_market_slab_free(slab, slot_idx);
        return OM_ERR_HASH_PUT;
    }
    kh_val(ladder->price_to_slot, it) = slot_idx;

    /* Find position and link into Q1 */
    uint32_t after_idx = om_ladder_find_insert_pos(slab, ladder, price, is_bid);
    om_ladder_link_after(slab, ladder, slot_idx, after_idx, is_bid);

    return 0;
}

/* Subtract quantity from ladder at price.
 * If qty reaches 0, unlink from Q1, remove from hash, and free slot.
 * Returns 0 on success.
 */
static int om_ladder_sub_qty(OmMarketLevelSlab *slab,
                              OmMarketLadder *ladder,
                              uint64_t price,
                              uint64_t qty,
                              bool is_bid) {
    if (qty == 0) {
        return 0;
    }

    /* Find slot in hash map */
    khiter_t it = kh_get(om_market_level_map, ladder->price_to_slot, price);
    if (it == kh_end(ladder->price_to_slot)) {
        return 0;  /* Price not in ladder */
    }

    uint32_t slot_idx = kh_val(ladder->price_to_slot, it);
    OmMarketLevelSlot *slot = &slab->slots[slot_idx];

    if (qty >= slot->qty) {
        /* Remove level entirely */
        om_ladder_unlink(slab, ladder, slot_idx, is_bid);
        kh_del(om_market_level_map, ladder->price_to_slot, it);
        om_market_slab_free(slab, slot_idx);
    } else {
        slot->qty -= qty;
    }

    return 0;
}

/* Get quantity at price. Returns true if found. */
static bool om_ladder_get_qty(const OmMarketLevelSlab *slab,
                               const OmMarketLadder *ladder,
                               uint64_t price,
                               uint64_t *out_qty) {
    khiter_t it = kh_get(om_market_level_map, ladder->price_to_slot, price);
    if (it == kh_end(ladder->price_to_slot)) {
        return false;
    }

    uint32_t slot_idx = kh_val(ladder->price_to_slot, it);
    *out_qty = slab->slots[slot_idx].qty;
    return true;
}

/* ============================================================================
 * Private Worker Implementation
 * ============================================================================ */

static int om_market_worker_init(OmMarketWorker *worker,
                                 uint32_t worker_id,
                                 uint16_t max_products,
                                 const OmMarketSubscription *subs,
                                 uint32_t sub_count,
                                 size_t expected_orders,
                                 uint32_t top_levels,
                                 uint32_t slab_capacity,
                                 OmMarketDealableFn dealable,
                                 void *dealable_ctx) {
    memset(worker, 0, sizeof(*worker));
    worker->worker_id = worker_id;
    worker->max_products = max_products;
    worker->subscription_count = sub_count;
    worker->dealable = dealable;
    worker->dealable_ctx = dealable_ctx;
    worker->top_levels = top_levels;

    worker->product_offsets = calloc((size_t)max_products + 1U, sizeof(*worker->product_offsets));
    if (!worker->product_offsets) {
        return OM_ERR_PRODUCT_OFFSET;
    }
    for (uint32_t i = 0; i < sub_count; i++) {
        if (subs[i].product_id < max_products) {
            worker->product_offsets[subs[i].product_id + 1U]++;
        }
    }
    for (uint32_t i = 1; i <= max_products; i++) {
        worker->product_offsets[i] += worker->product_offsets[i - 1U];
    }

    worker->product_orgs = calloc(sub_count, sizeof(*worker->product_orgs));
    if (!worker->product_orgs) {
        om_market_worker_destroy(worker);
        return OM_ERR_PRODUCT_ORGS;
    }

    worker->product_ladder_indices = calloc(sub_count, sizeof(*worker->product_ladder_indices));
    if (!worker->product_ladder_indices) {
        om_market_worker_destroy(worker);
        return OM_ERR_PRODUCT_ORGS;
    }

    worker->product_has_subs = calloc((size_t)max_products, sizeof(*worker->product_has_subs));
    if (!worker->product_has_subs) {
        om_market_worker_destroy(worker);
        return OM_ERR_PRODUCT_SUBS;
    }

    worker->org_ids = calloc(sub_count, sizeof(*worker->org_ids));
    if (!worker->org_ids) {
        om_market_worker_destroy(worker);
        return OM_ERR_ORG_IDS_ALLOC;
    }
    worker->org_index_map = calloc((size_t)UINT16_MAX + 1U, sizeof(*worker->org_index_map));
    if (!worker->org_index_map) {
        om_market_worker_destroy(worker);
        return OM_ERR_ORG_INDEX_ALLOC;
    }
    for (uint32_t i = 0; i <= UINT16_MAX; i++) {
        worker->org_index_map[i] = UINT32_MAX;
    }

    uint32_t *cursor = calloc((size_t)max_products, sizeof(*cursor));
    if (!cursor) {
        om_market_worker_destroy(worker);
        return OM_ERR_ALLOC_FAILED;
    }
    memcpy(cursor, worker->product_offsets, sizeof(*cursor) * max_products);
    for (uint32_t i = 0; i < sub_count; i++) {
        if (subs[i].product_id >= max_products) {
            continue;
        }
        uint32_t idx = cursor[subs[i].product_id]++;
        worker->product_orgs[idx] = subs[i].org_id;
        worker->product_has_subs[subs[i].product_id] = 1U;
        if (worker->org_index_map[subs[i].org_id] == UINT32_MAX) {
            worker->org_index_map[subs[i].org_id] = worker->org_count;
            worker->org_ids[worker->org_count] = subs[i].org_id;
            worker->org_count++;
        }
    }
    free(cursor);

    /* Initialize product slab for this worker */
    int ret = om_market_slab_init(&worker->product_slab, slab_capacity);
    if (ret != 0) {
        om_market_worker_destroy(worker);
        return ret;
    }

    /* Allocate product ladder array - one per product */
    worker->product_ladders = calloc((size_t)max_products, sizeof(*worker->product_ladders));
    if (!worker->product_ladders) {
        om_market_worker_destroy(worker);
        return OM_ERR_LADDER_ALLOC;
    }

    /* Initialize each product ladder */
    for (uint32_t i = 0; i < max_products; i++) {
        ret = om_ladder_init(&worker->product_ladders[i]);
        if (ret != 0) {
            om_market_worker_destroy(worker);
            return ret;
        }
    }

    /* Per-product order_id sets for O(k) query instead of O(K) */
    worker->product_order_sets = calloc((size_t)max_products, sizeof(*worker->product_order_sets));
    if (!worker->product_order_sets) {
        om_market_worker_destroy(worker);
        return OM_ERR_ALLOC_FAILED;
    }
    for (uint32_t i = 0; i < max_products; i++) {
        worker->product_order_sets[i] = kh_init(om_market_order_set);
        if (!worker->product_order_sets[i]) {
            om_market_worker_destroy(worker);
            return OM_ERR_HASH_INIT;
        }
    }

    /* Global orders map for product ladder tracking */
    worker->global_orders = kh_init(om_market_order_map);
    if (!worker->global_orders) {
        om_market_worker_destroy(worker);
        return OM_ERR_HASH_INIT;
    }
    if (expected_orders > 0) {
        kh_resize(om_market_order_map, worker->global_orders, expected_orders);
    }

    worker->scratch_qty_map = kh_init(om_market_qty_map);
    if (!worker->scratch_qty_map) {
        om_market_worker_destroy(worker);
        return OM_ERR_HASH_INIT;
    }

    /* Cache-line aligned dirty flags to prevent false sharing between workers */
    worker->ladder_dirty = om_aligned_calloc(sub_count, sizeof(*worker->ladder_dirty));
    if (!worker->ladder_dirty) {
        om_market_worker_destroy(worker);
        return OM_ERR_LADDER_DIRTY;
    }
    /* Dirty work-list + reverse (org,product) map, keyed by ladder index. The
     * ladder index of subs[i] is i (see pair_to_ladder/ladder_index below), so
     * the reverse map is just the per-worker subscription order. */
    worker->dirty_list = calloc(sub_count > 0 ? sub_count : 1U, sizeof(*worker->dirty_list));
    worker->ladder_org = calloc(sub_count > 0 ? sub_count : 1U, sizeof(*worker->ladder_org));
    worker->ladder_product = calloc(sub_count > 0 ? sub_count : 1U, sizeof(*worker->ladder_product));
    if (!worker->dirty_list || !worker->ladder_org || !worker->ladder_product) {
        om_market_worker_destroy(worker);
        return OM_ERR_LADDER_DIRTY;
    }
    worker->dirty_count = 0;
    for (uint32_t i = 0; i < sub_count; i++) {
        worker->ladder_org[i] = subs[i].org_id;
        worker->ladder_product[i] = subs[i].product_id;
    }
    worker->ladder_deltas = calloc(sub_count * 2U, sizeof(*worker->ladder_deltas));
    if (!worker->ladder_deltas) {
        om_market_worker_destroy(worker);
        return OM_ERR_LADDER_DELTA;
    }
    for (uint32_t i = 0; i < sub_count * 2U; i++) {
        worker->ladder_deltas[i] = kh_init(om_market_delta_map);
        if (!worker->ladder_deltas[i]) {
            om_market_worker_destroy(worker);
            return OM_ERR_HASH_INIT;
        }
    }

    worker->pair_to_ladder = kh_init(om_market_pair_map);
    if (!worker->pair_to_ladder) {
        om_market_worker_destroy(worker);
        return OM_ERR_HASH_INIT;
    }
    for (uint32_t i = 0; i < sub_count; i++) {
        uint32_t key = om_market_pair_key(subs[i].org_id, subs[i].product_id);
        int hret = 0;
        khiter_t it = kh_put(om_market_pair_map, worker->pair_to_ladder, key, &hret);
        if (hret < 0) {
            om_market_worker_destroy(worker);
            return OM_ERR_HASH_PUT;
        }
        kh_val(worker->pair_to_ladder, it) = i;
    }

    worker->ladder_index_stride = (size_t)max_products;
    size_t ladder_index_size = (size_t)worker->org_count * worker->ladder_index_stride;
    worker->ladder_index = calloc(ladder_index_size, sizeof(*worker->ladder_index));
    if (!worker->ladder_index) {
        om_market_worker_destroy(worker);
        return OM_ERR_INDEX_ALLOC;
    }
    for (size_t i = 0; i < ladder_index_size; i++) {
        worker->ladder_index[i] = UINT32_MAX;
    }

    for (uint32_t i = 0; i < sub_count; i++) {
        uint32_t org_index = worker->org_index_map[subs[i].org_id];
        if (org_index == UINT32_MAX) {
            continue;
        }
        worker->ladder_index[(size_t)org_index * worker->ladder_index_stride + subs[i].product_id] = i;
    }

    for (uint32_t product_id = 0; product_id < max_products; product_id++) {
        uint32_t start = worker->product_offsets[product_id];
        uint32_t end = worker->product_offsets[product_id + 1U];
        for (uint32_t idx = start; idx < end; idx++) {
            uint16_t org_id = worker->product_orgs[idx];
            uint32_t org_index = worker->org_index_map[org_id];
            if (org_index == UINT32_MAX) {
                worker->product_ladder_indices[idx] = UINT32_MAX;
                continue;
            }
            size_t map_idx = (size_t)org_index * worker->ladder_index_stride + product_id;
            worker->product_ladder_indices[idx] = worker->ladder_index[map_idx];
        }
    }

    return 0;
}

/* ============================================================================
 * Public Worker Implementation
 * ============================================================================ */

static int om_market_public_worker_init(OmMarketPublicWorker *worker,
                                        uint16_t max_products,
                                        uint32_t top_levels,
                                        uint32_t slab_capacity,
                                        size_t expected_orders) {
    memset(worker, 0, sizeof(*worker));
    worker->max_products = max_products;
    worker->top_levels = top_levels;

    worker->product_has_subs = calloc((size_t)max_products, sizeof(*worker->product_has_subs));
    if (!worker->product_has_subs) {
        return OM_ERR_PRODUCT_SUBS;
    }

    /* Initialize slab for this worker */
    int ret = om_market_slab_init(&worker->slab, slab_capacity);
    if (ret != 0) {
        om_market_public_worker_destroy(worker);
        return ret;
    }

    worker->ladders = calloc((size_t)max_products, sizeof(*worker->ladders));
    if (!worker->ladders) {
        om_market_public_worker_destroy(worker);
        return OM_ERR_LADDER_ALLOC;
    }

    /* Initialize each ladder */
    for (uint32_t i = 0; i < max_products; i++) {
        ret = om_ladder_init(&worker->ladders[i]);
        if (ret != 0) {
            om_market_public_worker_destroy(worker);
            return ret;
        }
    }

    /* Cache-line aligned dirty flags to prevent false sharing between workers */
    worker->dirty = om_aligned_calloc((size_t)max_products, sizeof(*worker->dirty));
    if (!worker->dirty) {
        om_market_public_worker_destroy(worker);
        return OM_ERR_LADDER_DIRTY;
    }
    /* Dirty work-list so a flush walks only changed products (O(dirty)). */
    worker->dirty_list = calloc(max_products > 0 ? (size_t)max_products : 1U, sizeof(*worker->dirty_list));
    if (!worker->dirty_list) {
        om_market_public_worker_destroy(worker);
        return OM_ERR_LADDER_DIRTY;
    }
    worker->dirty_count = 0;
    worker->deltas = calloc((size_t)max_products * 2U, sizeof(*worker->deltas));
    if (!worker->deltas) {
        om_market_public_worker_destroy(worker);
        return OM_ERR_LADDER_DELTA;
    }
    for (uint32_t i = 0; i < (uint32_t)max_products * 2U; i++) {
        worker->deltas[i] = kh_init(om_market_delta_map);
        if (!worker->deltas[i]) {
            om_market_public_worker_destroy(worker);
            return OM_ERR_HASH_INIT;
        }
    }
    worker->orders = kh_init(om_market_order_map);
    if (!worker->orders) {
        om_market_public_worker_destroy(worker);
        return OM_ERR_HASH_INIT;
    }
    if (expected_orders > 0) {
        kh_resize(om_market_order_map, worker->orders, expected_orders);
    }
    return 0;
}

/* ============================================================================
 * Destroy Functions
 * ============================================================================ */

static void om_market_public_worker_destroy(OmMarketPublicWorker *worker) {
    if (!worker) {
        return;
    }
    /* Destroy each ladder's hash map */
    if (worker->ladders) {
        for (uint32_t i = 0; i < worker->max_products; i++) {
            om_ladder_destroy(&worker->ladders[i]);
        }
    }
    om_market_slab_destroy(&worker->slab);
    if (worker->deltas) {
        for (uint32_t i = 0; i < worker->max_products * 2U; i++) {
            if (worker->deltas[i]) {
                kh_destroy(om_market_delta_map, worker->deltas[i]);
            }
        }
    }
    if (worker->orders) {
        kh_destroy(om_market_order_map, worker->orders);
    }
    free(worker->deltas);
    free(worker->dirty);
    free(worker->dirty_list);
    free(worker->ladders);
    free(worker->product_has_subs);
    memset(worker, 0, sizeof(*worker));
}

static void om_market_worker_destroy(OmMarketWorker *worker) {
    if (!worker) {
        return;
    }
    /* Destroy each product ladder's hash map */
    if (worker->product_ladders) {
        for (uint32_t i = 0; i < worker->max_products; i++) {
            om_ladder_destroy(&worker->product_ladders[i]);
        }
    }
    /* Destroy per-product order sets */
    if (worker->product_order_sets) {
        for (uint32_t i = 0; i < worker->max_products; i++) {
            if (worker->product_order_sets[i]) {
                kh_destroy(om_market_order_set, worker->product_order_sets[i]);
            }
        }
    }
    free(worker->product_order_sets);
    om_market_slab_destroy(&worker->product_slab);
    if (worker->global_orders) {
        kh_destroy(om_market_order_map, worker->global_orders);
    }
    if (worker->scratch_qty_map) {
        kh_destroy(om_market_qty_map, worker->scratch_qty_map);
    }
    if (worker->pair_to_ladder) {
        kh_destroy(om_market_pair_map, worker->pair_to_ladder);
    }
    free(worker->ladder_index);
    free(worker->ladder_dirty);
    free(worker->dirty_list);
    free(worker->ladder_org);
    free(worker->ladder_product);
    if (worker->ladder_deltas) {
        for (uint32_t i = 0; i < worker->subscription_count * 2U; i++) {
            if (worker->ladder_deltas[i]) {
                kh_destroy(om_market_delta_map, worker->ladder_deltas[i]);
            }
        }
    }
    free(worker->product_has_subs);
    free(worker->ladder_deltas);
    free(worker->product_offsets);
    free(worker->product_orgs);
    free(worker->product_ladder_indices);
    free(worker->org_ids);
    free(worker->org_index_map);
    free(worker->product_ladders);
    memset(worker, 0, sizeof(*worker));
}

/* ============================================================================
 * Market Init/Destroy
 * ============================================================================ */

int om_market_init(OmMarket *market, const OmMarketConfig *config) {
    if (!market || !config || !config->org_to_worker || !config->subs || config->sub_count == 0) {
        return OM_ERR_NULL_PARAM;
    }
    if (config->worker_count == 0 || config->max_products == 0) {
        return OM_ERR_INVALID_PARAM;
    }
    if (!config->dealable) {
        return OM_ERR_NO_DEALABLE_CB;
    }

    memset(market, 0, sizeof(*market));
    market->worker_count = config->worker_count;
    market->public_worker_count = config->public_worker_count;
    market->max_products = config->max_products;
    market->top_levels = config->top_levels;
    market->dealable = config->dealable;
    market->dealable_ctx = config->dealable_ctx;

    market->workers = calloc(config->worker_count, sizeof(*market->workers));
    if (!market->workers) {
        return OM_ERR_ALLOC_FAILED;
    }
    market->public_workers = calloc(config->public_worker_count, sizeof(*market->public_workers));
    if (!market->public_workers) {
        free(market->workers);
        market->workers = NULL;
        return OM_ERR_ALLOC_FAILED;
    }

    uint32_t *counts = calloc(config->worker_count, sizeof(*counts));
    if (!counts) {
        free(market->public_workers);
        free(market->workers);
        market->public_workers = NULL;
        market->workers = NULL;
        return OM_ERR_ALLOC_FAILED;
    }
    for (uint32_t i = 0; i < config->sub_count; i++) {
        uint16_t org_id = config->subs[i].org_id;
        uint32_t worker_id = config->org_to_worker[org_id];
        if (worker_id >= config->worker_count) {
            free(counts);
            free(market->public_workers);
            free(market->workers);
            market->public_workers = NULL;
            market->workers = NULL;
            return OM_ERR_WORKER_ID_RANGE;
        }
        counts[worker_id]++;
    }

    OmMarketSubscription *buckets = calloc(config->sub_count, sizeof(*buckets));
    if (!buckets) {
        free(counts);
        free(market->public_workers);
        free(market->workers);
        market->public_workers = NULL;
        market->workers = NULL;
        return OM_ERR_ALLOC_FAILED;
    }
    uint32_t *offsets = calloc(config->worker_count, sizeof(*offsets));
    if (!offsets) {
        free(buckets);
        free(counts);
        free(market->public_workers);
        free(market->workers);
        market->public_workers = NULL;
        market->workers = NULL;
        return OM_ERR_ALLOC_FAILED;
    }
    uint32_t total = 0;
    for (uint32_t w = 0; w < config->worker_count; w++) {
        offsets[w] = total;
        total += counts[w];
    }
    for (uint32_t i = 0; i < config->sub_count; i++) {
        uint16_t org_id = config->subs[i].org_id;
        uint32_t worker_id = config->org_to_worker[org_id];
        buckets[offsets[worker_id]++] = config->subs[i];
    }

    /* Calculate slab capacity per worker:
     * Product-level slab: max_products * expected_levels * 2 sides * 1.5 safety
     * (no per-org slabs needed — org qty is stored in hash maps)
     */
    uint32_t expected_levels = config->expected_price_levels > 0
                                   ? (uint32_t)config->expected_price_levels
                                   : 50;  /* Default 50 levels per side */
    uint32_t safety_factor_x10 = 15;  /* 1.5x safety factor */

    total = 0;
    for (uint32_t w = 0; w < config->worker_count; w++) {
        uint32_t count = counts[w];
        /* Slab capacity: max_products * expected_levels * 2 sides * 1.5 safety */
        uint32_t slab_cap = (config->max_products * expected_levels * 2 * safety_factor_x10) / 10;
        if (slab_cap < 64) {
            slab_cap = 64;  /* Minimum slab size */
        }
        int ret = om_market_worker_init(&market->workers[w], w, config->max_products,
                                        buckets + total, count,
                                        config->expected_orders_per_worker,
                                        config->top_levels,
                                        slab_cap,
                                        config->dealable,
                                        config->dealable_ctx);
        if (ret != 0) {
            free(buckets);
            free(offsets);
            free(counts);
            om_market_destroy(market);
            return ret;
        }
        total += count;
    }

    if (!config->product_to_public_worker || config->public_worker_count == 0) {
        free(buckets);
        free(offsets);
        free(counts);
        om_market_destroy(market);
        return OM_ERR_NO_PUBLIC_MAP;
    }

    /* Public worker slab capacity: max_products * expected_levels * 2 sides * 1.5 safety */
    uint32_t pub_slab_cap = (config->max_products * expected_levels * 2 * safety_factor_x10) / 10;
    if (pub_slab_cap < 64) {
        pub_slab_cap = 64;
    }

    for (uint32_t w = 0; w < config->public_worker_count; w++) {
        int ret = om_market_public_worker_init(&market->public_workers[w], config->max_products,
                                               config->top_levels,
                                               pub_slab_cap,
                                               config->expected_orders_per_worker);
        if (ret != 0) {
            free(buckets);
            free(offsets);
            free(counts);
            om_market_destroy(market);
            return ret;
        }
    }

    uint8_t *public_products = calloc((size_t)config->max_products, sizeof(*public_products));
    if (!public_products) {
        free(buckets);
        free(offsets);
        free(counts);
        om_market_destroy(market);
        return OM_ERR_PUBLIC_ALLOC;
    }
    for (uint32_t i = 0; i < config->sub_count; i++) {
        uint16_t product_id = config->subs[i].product_id;
        if (product_id < config->max_products) {
            public_products[product_id] = 1U;
        }
    }
    for (uint32_t product_id = 0; product_id < config->max_products; product_id++) {
        if (!public_products[product_id]) {
            continue;
        }
        uint32_t worker_id = config->product_to_public_worker[product_id];
        if (worker_id >= config->public_worker_count) {
            free(public_products);
            free(buckets);
            free(offsets);
            free(counts);
            om_market_destroy(market);
            return OM_ERR_WORKER_ID_RANGE;
        }
        market->public_workers[worker_id].product_has_subs[product_id] = 1U;
    }
    free(public_products);

    free(buckets);
    free(offsets);
    free(counts);
    return 0;
}

void om_market_destroy(OmMarket *market) {
    if (!market) {
        return;
    }
    for (uint32_t i = 0; i < market->worker_count; i++) {
        om_market_worker_destroy(&market->workers[i]);
    }
    for (uint32_t i = 0; i < market->public_worker_count; i++) {
        om_market_public_worker_destroy(&market->public_workers[i]);
    }
    free(market->workers);
    free(market->public_workers);
    memset(market, 0, sizeof(*market));
}

OmMarketWorker *om_market_worker(OmMarket *market, uint32_t worker_id) {
    if (!market || worker_id >= market->worker_count) {
        return NULL;
    }
    return &market->workers[worker_id];
}

/* ============================================================================
 * Helper Functions
 * ============================================================================ */

static int om_market_worker_find_ladder(const OmMarketWorker *worker,
                                        uint16_t org_id,
                                        uint16_t product_id,
                                        uint32_t *out_index) {
    if (!worker || !out_index) {
        return OM_ERR_NULL_PARAM;
    }
    uint32_t org_index = worker->org_index_map[org_id];
    if (org_index == UINT32_MAX) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    size_t idx = (size_t)org_index * worker->ladder_index_stride + product_id;
    uint32_t ladder_idx = worker->ladder_index[idx];
    if (ladder_idx == UINT32_MAX) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    *out_index = ladder_idx;
    return 0;
}

static void om_market_ladder_mark_dirty(OmMarketWorker *worker, uint32_t ladder_idx) {
    if (worker && worker->ladder_dirty && ladder_idx < worker->subscription_count) {
        /* Append to the work-list only on the clean->dirty edge, so an entry is
         * listed at most once between compactions and the list can never exceed
         * subscription_count. */
        if (!worker->ladder_dirty[ladder_idx]) {
            worker->ladder_dirty[ladder_idx] = 1U;
            if (worker->dirty_list && worker->dirty_count < worker->subscription_count) {
                worker->dirty_list[worker->dirty_count++] = ladder_idx;
            }
        }
    }
}

static void om_market_public_mark_dirty(OmMarketPublicWorker *worker, uint16_t product_id) {
    if (worker && worker->dirty && product_id < worker->max_products) {
        if (!worker->dirty[product_id]) {
            worker->dirty[product_id] = 1U;
            if (worker->dirty_list && worker->dirty_count < worker->max_products) {
                worker->dirty_list[worker->dirty_count++] = product_id;
            }
        }
    }
}

static khash_t(om_market_delta_map) *om_market_delta_for_ladder(OmMarketWorker *worker,
                                                                 uint32_t ladder_idx,
                                                                 bool bids) {
    if (!worker || !worker->ladder_deltas || ladder_idx >= worker->subscription_count) {
        return NULL;
    }
    uint32_t idx = ladder_idx * 2U + (bids ? 0U : 1U);
    return worker->ladder_deltas[idx];
}

static khash_t(om_market_delta_map) *om_market_delta_for_public(OmMarketPublicWorker *worker,
                                                                 uint16_t product_id,
                                                                 bool bids) {
    if (!worker || !worker->deltas || product_id >= worker->max_products) {
        return NULL;
    }
    uint32_t idx = (uint32_t)product_id * 2U + (bids ? 0U : 1U);
    return worker->deltas[idx];
}

/* Accumulate both the per-flush signed delta and the running absolute per-viewer
 * qty in the SAME khash entry — one map per (ladder,side), no second map. `delta`
 * is the signed change since last flush; `abs_qty_delta` is the signed change to
 * the running absolute qty (same value for insert/activate/cancel/match — the
 * viewer's dealable qty moved by exactly the delta). Maintaining abs_qty here,
 * in the entry the process path already touches, lets copy_deltas report it O(1)
 * at flush and kills the O(orders-in-product) order-set walk get_qty did. */
static void om_market_delta_add(khash_t(om_market_delta_map) *map,
                                uint64_t price,
                                int64_t delta,
                                int64_t abs_qty_delta) {
    if (!map || (delta == 0 && abs_qty_delta == 0)) {
        return;
    }
    int ret = 0;
    khiter_t it = kh_get(om_market_delta_map, map, price);
    if (it == kh_end(map)) {
        it = kh_put(om_market_delta_map, map, price, &ret);
        if (ret < 0) {
            return;
        }
        kh_val(map, it).delta = delta;
        kh_val(map, it).abs_qty = (abs_qty_delta > 0) ? (uint64_t)abs_qty_delta : 0U;
        if (kh_val(map, it).abs_qty == 0U && delta == 0) {
            kh_del(om_market_delta_map, map, it);
        }
        return;
    }
    kh_val(map, it).delta += delta;
    uint64_t cur = kh_val(map, it).abs_qty;
    if (abs_qty_delta < 0 && (uint64_t)(-abs_qty_delta) > cur) {
        kh_val(map, it).abs_qty = 0U;
    } else {
        kh_val(map, it).abs_qty = cur + (uint64_t)abs_qty_delta;
    }
    if (kh_val(map, it).delta == 0 && kh_val(map, it).abs_qty == 0U) {
        kh_del(om_market_delta_map, map, it);
    }
}

/* Compute per-org dealable qty from global order state + dealable callback.
 * Formula: max(0, min(vol_remain, dealable(rec, viewer)) - (vol_remain - remaining))
 */
static uint64_t om_market_compute_org_qty(const OmMarketWorker *worker,
                                           const OmMarketOrderState *state,
                                           uint64_t order_id,
                                           uint16_t viewer_org) {
    if (!worker->dealable) return 0;
    OmWalInsert fake = {
        .order_id = order_id,
        .price = state->price,
        .volume = state->vol_remain,
        .vol_remain = state->vol_remain,
        .org = state->org,
        .flags = state->flags,
        .product_id = state->product_id,
    };
    uint64_t dq = worker->dealable(&fake, viewer_org, worker->dealable_ctx);
    if (dq == 0) return 0;
    uint64_t cap = state->vol_remain < dq ? state->vol_remain : dq;
    uint64_t matched = state->vol_remain - state->remaining;
    return cap > matched ? cap - matched : 0;
}


/* Compute qty from a precomputed dealable value + remaining. */
static inline uint64_t _om_market_qty_from_dq(uint64_t vol_remain, uint64_t dq,
                                               uint64_t remaining) {
    if (dq == 0) return 0;
    uint64_t cap = vol_remain < dq ? vol_remain : dq;
    uint64_t matched = vol_remain - remaining;
    return cap > matched ? cap - matched : 0;
}

/* ============================================================================
 * Process Functions
 * ============================================================================ */

int om_market_worker_process(OmMarketWorker *worker, OmWalType type, const void *data) {
    if (!worker || !data) {
        return OM_ERR_NULL_PARAM;
    }

    switch (type) {
        case OM_WAL_INSERT: {
            const OmWalInsert *rec = (const OmWalInsert *)data;
            if (worker->product_has_subs && !worker->product_has_subs[rec->product_id]) {
                return 0;
            }
            bool is_bid = OM_IS_BID(rec->flags);
            uint16_t side = OM_GET_SIDE(rec->flags);

            /* 1. Update product ladder */
            om_ladder_add_qty(&worker->product_slab,
                              &worker->product_ladders[rec->product_id],
                              rec->price, rec->vol_remain, is_bid);

            /* 2. Record in global_orders with org/flags/vol_remain */
            int gret = 0;
            khiter_t git = kh_put(om_market_order_map, worker->global_orders,
                                  rec->order_id, &gret);
            if (gret < 0) return OM_ERR_HASH_PUT;
            kh_val(worker->global_orders, git) = (OmMarketOrderState){
                .product_id = rec->product_id,
                .side = side,
                .active = true,
                .org = rec->org,
                .flags = rec->flags,
                .price = rec->price,
                .remaining = rec->vol_remain,
                .vol_remain = rec->vol_remain
            };

            /* 3. Add to per-product order set */
            int sret = 0;
            kh_put(om_market_order_set, worker->product_order_sets[rec->product_id],
                   rec->order_id, &sret);

            /* 4. Fan-out: call dealable directly (no fake OmWalInsert needed) */
            uint32_t start = worker->product_offsets[rec->product_id];
            uint32_t end = worker->product_offsets[rec->product_id + 1U];
            for (uint32_t idx = start; idx < end; idx++) {
                uint16_t viewer_org = worker->product_orgs[idx];
                uint32_t ladder_idx = worker->product_ladder_indices[idx];
                if (ladder_idx == UINT32_MAX) {
                    continue;
                }
                uint64_t dq = worker->dealable(rec, viewer_org, worker->dealable_ctx);
                if (dq == 0) continue;
                uint64_t qty = rec->vol_remain < dq ? rec->vol_remain : dq;

                khash_t(om_market_delta_map) *delta_map =
                    om_market_delta_for_ladder(worker, ladder_idx, is_bid);
                om_market_delta_add(delta_map, rec->price, (int64_t)qty, (int64_t)qty);
                om_market_ladder_mark_dirty(worker, ladder_idx);
            }
            return 0;
        }
        case OM_WAL_CANCEL:
        case OM_WAL_DEACTIVATE: {
            const OmWalCancel *rec = (const OmWalCancel *)data;

            /* 1. Lookup global order */
            khiter_t git = kh_get(om_market_order_map, worker->global_orders, rec->order_id);
            if (git == kh_end(worker->global_orders)) {
                return 0;
            }
            OmMarketOrderState *gstate = &kh_val(worker->global_orders, git);
            if (!gstate->active) {
                return 0;
            }

            bool is_bid = gstate->side == OM_SIDE_BID;

            /* 2. Fan-out FIRST (needs pre-cancel remaining) */
            uint32_t start = worker->product_offsets[gstate->product_id];
            uint32_t end = worker->product_offsets[gstate->product_id + 1U];
            for (uint32_t idx = start; idx < end; idx++) {
                uint16_t viewer_org = worker->product_orgs[idx];
                uint32_t ladder_idx = worker->product_ladder_indices[idx];
                if (ladder_idx == UINT32_MAX) {
                    continue;
                }
                uint64_t pre_qty = om_market_compute_org_qty(worker, gstate, rec->order_id, viewer_org);
                if (pre_qty == 0) {
                    continue;
                }

                khash_t(om_market_delta_map) *delta_map =
                    om_market_delta_for_ladder(worker, ladder_idx, is_bid);
                om_market_delta_add(delta_map, gstate->price, -(int64_t)pre_qty, -(int64_t)pre_qty);
                om_market_ladder_mark_dirty(worker, ladder_idx);
            }

            /* 3. THEN update product ladder + mark global inactive */
            om_ladder_sub_qty(&worker->product_slab,
                              &worker->product_ladders[gstate->product_id],
                              gstate->price, gstate->remaining, is_bid);

            /* 4. Remove from per-product order set */
            khiter_t sit = kh_get(om_market_order_set,
                                  worker->product_order_sets[gstate->product_id],
                                  rec->order_id);
            if (sit != kh_end(worker->product_order_sets[gstate->product_id])) {
                kh_del(om_market_order_set, worker->product_order_sets[gstate->product_id], sit);
            }

            gstate->remaining = 0;
            gstate->active = false;
            /* Terminal: a cancelled/deactivated order has remaining==0, so ACTIVATE
             * can never restore it (see test_market_deactivate_activate_semantics)
             * and no later event references it. Evict it from global_orders so the
             * map tracks only live orders instead of growing with every order ever
             * seen (the mktman RSS-growth source). */
            kh_del(om_market_order_map, worker->global_orders, git);
            return 0;
        }
        case OM_WAL_ACTIVATE: {
            const OmWalActivate *rec = (const OmWalActivate *)data;

            /* 1. Lookup global order */
            khiter_t git = kh_get(om_market_order_map, worker->global_orders, rec->order_id);
            if (git == kh_end(worker->global_orders)) {
                return 0;
            }
            OmMarketOrderState *gstate = &kh_val(worker->global_orders, git);
            if (gstate->active || gstate->remaining == 0) {
                return 0;
            }

            /* 2. Mark active + update product ladder */
            bool is_bid = gstate->side == OM_SIDE_BID;
            gstate->active = true;
            om_ladder_add_qty(&worker->product_slab,
                              &worker->product_ladders[gstate->product_id],
                              gstate->price, gstate->remaining, is_bid);

            /* 3. Fan-out: compute per-org qty, record delta */
            uint32_t start = worker->product_offsets[gstate->product_id];
            uint32_t end = worker->product_offsets[gstate->product_id + 1U];
            for (uint32_t idx = start; idx < end; idx++) {
                uint16_t viewer_org = worker->product_orgs[idx];
                uint32_t ladder_idx = worker->product_ladder_indices[idx];
                if (ladder_idx == UINT32_MAX) {
                    continue;
                }
                uint64_t qty = om_market_compute_org_qty(worker, gstate, rec->order_id, viewer_org);
                if (qty == 0) {
                    continue;
                }

                khash_t(om_market_delta_map) *delta_map =
                    om_market_delta_for_ladder(worker, ladder_idx, is_bid);
                om_market_delta_add(delta_map, gstate->price, (int64_t)qty, (int64_t)qty);
                om_market_ladder_mark_dirty(worker, ladder_idx);
            }
            return 0;
        }
        case OM_WAL_MATCH: {
            const OmWalMatch *rec = (const OmWalMatch *)data;

            /* 1. Lookup global order */
            khiter_t git = kh_get(om_market_order_map, worker->global_orders, rec->maker_id);
            if (git == kh_end(worker->global_orders)) {
                return 0;
            }
            OmMarketOrderState *gstate = &kh_val(worker->global_orders, git);
            if (!gstate->active || gstate->remaining == 0) {
                return 0;
            }

            bool is_bid = gstate->side == OM_SIDE_BID;
            uint64_t global_match = rec->volume > gstate->remaining
                                        ? gstate->remaining : rec->volume;

            /* 2. Fan-out FIRST — single dealable() call per org */
            uint64_t pre_remaining = gstate->remaining;
            uint64_t post_remaining = pre_remaining - global_match;
            OmWalInsert fake = {
                .order_id = rec->maker_id,
                .price = gstate->price,
                .volume = gstate->vol_remain,
                .vol_remain = gstate->vol_remain,
                .org = gstate->org,
                .flags = gstate->flags,
                .product_id = gstate->product_id,
            };
            uint32_t start = worker->product_offsets[gstate->product_id];
            uint32_t end = worker->product_offsets[gstate->product_id + 1U];
            for (uint32_t idx = start; idx < end; idx++) {
                uint16_t viewer_org = worker->product_orgs[idx];
                uint32_t ladder_idx = worker->product_ladder_indices[idx];
                if (ladder_idx == UINT32_MAX) {
                    continue;
                }
                uint64_t dq = worker->dealable(&fake, viewer_org, worker->dealable_ctx);
                uint64_t pre_qty = _om_market_qty_from_dq(gstate->vol_remain, dq, pre_remaining);
                uint64_t post_qty = _om_market_qty_from_dq(gstate->vol_remain, dq, post_remaining);
                int64_t delta = (int64_t)post_qty - (int64_t)pre_qty;
                if (delta == 0) {
                    continue;
                }

                khash_t(om_market_delta_map) *delta_map =
                    om_market_delta_for_ladder(worker, ladder_idx, is_bid);
                om_market_delta_add(delta_map, gstate->price, delta, delta);
                om_market_ladder_mark_dirty(worker, ladder_idx);
            }

            /* 3. THEN update product ladder + global remaining */
            om_ladder_sub_qty(&worker->product_slab,
                              &worker->product_ladders[gstate->product_id],
                              gstate->price, global_match, is_bid);
            gstate->remaining -= global_match;

            /* 4. Remove from per-product order set if fully matched */
            if (gstate->remaining == 0) {
                khiter_t sit = kh_get(om_market_order_set,
                                      worker->product_order_sets[gstate->product_id],
                                      rec->maker_id);
                if (sit != kh_end(worker->product_order_sets[gstate->product_id])) {
                    kh_del(om_market_order_set, worker->product_order_sets[gstate->product_id], sit);
                }
                /* Fully matched: terminal, evict from global_orders so the map
                 * tracks only live orders (bounds memory). */
                kh_del(om_market_order_map, worker->global_orders, git);
            }
            return 0;
        }
        default:
            return 0;
    }
}

int om_market_public_process(OmMarketPublicWorker *worker, OmWalType type, const void *data) {
    if (!worker || !data) {
        return OM_ERR_NULL_PARAM;
    }

    switch (type) {
        case OM_WAL_INSERT: {
            const OmWalInsert *rec = (const OmWalInsert *)data;
            if (worker->product_has_subs && !worker->product_has_subs[rec->product_id]) {
                return 0;
            }
            int pub_ret = 0;
            khiter_t pub_it = kh_put(om_market_order_map, worker->orders, rec->order_id, &pub_ret);
            if (pub_ret < 0) {
                return OM_ERR_HASH_PUT;
            }
            OmMarketOrderState pub_state = {
                .product_id = rec->product_id,
                .side = OM_GET_SIDE(rec->flags),
                .active = true,
                .price = rec->price,
                .remaining = rec->vol_remain
            };
            kh_val(worker->orders, pub_it) = pub_state;
            OmMarketLadder *ladder = &worker->ladders[rec->product_id];
            bool is_bid = OM_IS_BID(rec->flags);
            om_ladder_add_qty(&worker->slab, ladder, rec->price, rec->vol_remain, is_bid);
            khash_t(om_market_delta_map) *delta_map =
                om_market_delta_for_public(worker, rec->product_id, is_bid);
            om_market_delta_add(delta_map, rec->price, (int64_t)rec->vol_remain, 0);
            om_market_public_mark_dirty(worker, rec->product_id);
            return 0;
        }
        case OM_WAL_CANCEL:
        case OM_WAL_DEACTIVATE: {
            const OmWalCancel *rec = (const OmWalCancel *)data;
            khiter_t pub_it = kh_get(om_market_order_map, worker->orders, rec->order_id);
            if (pub_it == kh_end(worker->orders)) {
                return 0;
            }
            OmMarketOrderState *pub_state = &kh_val(worker->orders, pub_it);
            if (!pub_state->active || pub_state->remaining == 0) {
                return 0;
            }
            uint16_t product_id = pub_state->product_id;
            OmMarketLadder *ladder = &worker->ladders[product_id];
            bool is_bid = pub_state->side == OM_SIDE_BID;
            om_ladder_sub_qty(&worker->slab, ladder, pub_state->price, pub_state->remaining, is_bid);
            uint64_t removed = pub_state->remaining;
            pub_state->remaining = 0;
            pub_state->active = false;
            khash_t(om_market_delta_map) *delta_map =
                om_market_delta_for_public(worker, product_id, is_bid);
            om_market_delta_add(delta_map, pub_state->price, -(int64_t)removed, 0);
            om_market_public_mark_dirty(worker, product_id);
            /* Terminal (remaining==0): evict so the order map tracks only live
             * orders instead of every order ever seen (bounds memory). */
            kh_del(om_market_order_map, worker->orders, pub_it);
            return 0;
        }
        case OM_WAL_ACTIVATE: {
            const OmWalActivate *rec = (const OmWalActivate *)data;
            khiter_t pub_it = kh_get(om_market_order_map, worker->orders, rec->order_id);
            if (pub_it == kh_end(worker->orders)) {
                return 0;
            }
            OmMarketOrderState *pub_state = &kh_val(worker->orders, pub_it);
            if (pub_state->active || pub_state->remaining == 0) {
                return 0;
            }
            OmMarketLadder *ladder = &worker->ladders[pub_state->product_id];
            bool is_bid = pub_state->side == OM_SIDE_BID;
            uint64_t added = pub_state->remaining;
            om_ladder_add_qty(&worker->slab, ladder, pub_state->price, added, is_bid);
            pub_state->active = true;
            khash_t(om_market_delta_map) *delta_map =
                om_market_delta_for_public(worker, pub_state->product_id, is_bid);
            om_market_delta_add(delta_map, pub_state->price, (int64_t)added, 0);
            om_market_public_mark_dirty(worker, pub_state->product_id);
            return 0;
        }
        case OM_WAL_MATCH: {
            const OmWalMatch *rec = (const OmWalMatch *)data;
            khiter_t pub_it = kh_get(om_market_order_map, worker->orders, rec->maker_id);
            if (pub_it == kh_end(worker->orders)) {
                return 0;
            }
            OmMarketOrderState *pub_state = &kh_val(worker->orders, pub_it);
            if (!pub_state->active || pub_state->remaining == 0) {
                return 0;
            }
            uint16_t product_id = pub_state->product_id;
            OmMarketLadder *ladder = &worker->ladders[product_id];
            bool is_bid = pub_state->side == OM_SIDE_BID;
            uint64_t match_vol = rec->volume > pub_state->remaining
                                     ? pub_state->remaining
                                     : rec->volume;
            om_ladder_sub_qty(&worker->slab, ladder, pub_state->price, match_vol, is_bid);
            pub_state->remaining -= match_vol;
            khash_t(om_market_delta_map) *delta_map =
                om_market_delta_for_public(worker, product_id, is_bid);
            om_market_delta_add(delta_map, pub_state->price, -(int64_t)match_vol, 0);
            om_market_public_mark_dirty(worker, product_id);
            /* Fully matched: terminal, evict from the order map (bounds memory). */
            if (pub_state->remaining == 0) {
                kh_del(om_market_order_map, worker->orders, pub_it);
            }
            return 0;
        }
        default:
            return 0;
    }
}

/* ============================================================================
 * Query Functions
 * ============================================================================ */

int om_market_worker_get_qty(const OmMarketWorker *worker,
                             uint16_t org_id,
                             uint16_t product_id,
                             uint16_t side,
                             uint64_t price,
                             uint64_t *out_qty) {
    if (!worker || !out_qty) {
        return OM_ERR_NULL_PARAM;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    /* Iterate per-product order set — O(k) instead of O(K) */
    khash_t(om_market_order_set) *oset = worker->product_order_sets[product_id];
    uint64_t total = 0;
    for (khiter_t it = kh_begin(oset); it != kh_end(oset); ++it) {
        if (!kh_exist(oset, it)) continue;
        uint64_t order_id = kh_key(oset, it);
        khiter_t git = kh_get(om_market_order_map, worker->global_orders, order_id);
        if (git == kh_end(worker->global_orders)) continue;
        const OmMarketOrderState *state = &kh_val(worker->global_orders, git);
        if (!state->active || state->side != side || state->price != price) {
            continue;
        }
        total += om_market_compute_org_qty(worker, state, order_id, org_id);
    }
    if (total == 0) {
        return OM_ERR_NOT_FOUND;
    }
    *out_qty = total;
    return 0;
}

int om_market_public_get_qty(const OmMarketPublicWorker *worker,
                             uint16_t product_id,
                             uint16_t side,
                             uint64_t price,
                             uint64_t *out_qty) {
    if (!worker || !out_qty || !worker->ladders) {
        return OM_ERR_NULL_PARAM;
    }
    if (product_id >= worker->max_products) {
        return OM_ERR_OUT_OF_RANGE;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    const OmMarketLadder *ladder = &worker->ladders[product_id];
    (void)side;  /* Side not needed for hash lookup */

    if (om_ladder_get_qty(&worker->slab, ladder, price, out_qty)) {
        return 0;
    }
    return OM_ERR_NOT_FOUND;
}

int om_market_worker_is_subscribed(const OmMarketWorker *worker,
                                  uint16_t org_id,
                                  uint16_t product_id) {
    if (!worker) {
        return OM_ERR_NULL_PARAM;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return 0;
    }
    return 1;
}

/* ============================================================================
 * Delta Functions
 * ============================================================================ */

int om_market_worker_delta_count(const OmMarketWorker *worker,
                                 uint16_t org_id,
                                 uint16_t product_id,
                                 uint16_t side) {
    if (!worker || !worker->ladder_deltas) {
        return OM_ERR_NULL_PARAM;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    uint32_t idx = ladder_idx * 2U + (side == OM_SIDE_BID ? 0U : 1U);
    khash_t(om_market_delta_map) *map = worker->ladder_deltas[idx];
    return map ? (int)kh_size(map) : 0;
}

int om_market_worker_copy_deltas(const OmMarketWorker *worker,
                                 uint16_t org_id,
                                 uint16_t product_id,
                                 uint16_t side,
                                 OmMarketDelta *out,
                                 size_t max) {
    if (!worker || !out || max == 0 || !worker->ladder_deltas) {
        return OM_ERR_NULL_PARAM;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    return om_market_worker_copy_deltas_by_ladder(worker, ladder_idx, side, out, max);
}

int om_market_worker_copy_deltas_by_ladder(const OmMarketWorker *worker,
                                           uint32_t ladder_idx,
                                           uint16_t side,
                                           OmMarketDelta *out,
                                           size_t max) {
    if (!worker || !out || max == 0 || !worker->ladder_deltas ||
        ladder_idx >= worker->subscription_count) {
        return OM_ERR_NULL_PARAM;
    }
    uint32_t idx = ladder_idx * 2U + (side == OM_SIDE_BID ? 0U : 1U);
    khash_t(om_market_delta_map) *map = worker->ladder_deltas[idx];
    if (!map) {
        return 0;
    }
    /* abs_qty is maintained in the same entry the process path already touches,
     * so one khash walk yields both delta and abs_qty — no O(orders) walk. */
    size_t count = 0;
    for (khiter_t it = kh_begin(map); it != kh_end(map) && count < max; ++it) {
        if (!kh_exist(map, it)) {
            continue;
        }
        out[count].price = kh_key(map, it);
        out[count].delta = kh_val(map, it).delta;
        out[count].abs_qty = kh_val(map, it).abs_qty;
        count++;
    }
    return (int)count;
}

int om_market_worker_clear_deltas(OmMarketWorker *worker,
                                  uint16_t org_id,
                                  uint16_t product_id,
                                  uint16_t side) {
    if (!worker || !worker->ladder_deltas) {
        return OM_ERR_NULL_PARAM;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    return om_market_worker_clear_deltas_by_ladder(worker, ladder_idx, side);
}

int om_market_worker_clear_deltas_by_ladder(OmMarketWorker *worker,
                                            uint32_t ladder_idx,
                                            uint16_t side) {
    if (!worker || !worker->ladder_deltas ||
        ladder_idx >= worker->subscription_count) {
        return OM_ERR_NULL_PARAM;
    }
    uint32_t idx = ladder_idx * 2U + (side == OM_SIDE_BID ? 0U : 1U);
    khash_t(om_market_delta_map) *map = worker->ladder_deltas[idx];
    if (!map) {
        return 0;
    }
    /* Reset the whole map each flush (original semantic). The map only holds
     * this window's touched levels — bounded by the dirty work, same footprint
     * as before. abs_qty is re-accumulated by the process path each window from
     * the signed delta (which IS the abs_qty change for that event), so a fresh
     * map still yields correct abs_qty at flush without the order-set walk. */
    kh_clear(om_market_delta_map, map);
    return 0;
}

int om_market_public_delta_count(const OmMarketPublicWorker *worker,
                                 uint16_t product_id,
                                 uint16_t side) {
    if (!worker || !worker->deltas) {
        return OM_ERR_NULL_PARAM;
    }
    if (product_id >= worker->max_products) {
        return OM_ERR_OUT_OF_RANGE;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    uint32_t idx = (uint32_t)product_id * 2U + (side == OM_SIDE_BID ? 0U : 1U);
    khash_t(om_market_delta_map) *map = worker->deltas[idx];
    return map ? (int)kh_size(map) : 0;
}

int om_market_public_copy_deltas(const OmMarketPublicWorker *worker,
                                 uint16_t product_id,
                                 uint16_t side,
                                 OmMarketDelta *out,
                                 size_t max) {
    if (!worker || !out || !worker->deltas) {
        return OM_ERR_NULL_PARAM;
    }
    if (max == 0) {
        return OM_ERR_INVALID_PARAM;
    }
    if (product_id >= worker->max_products) {
        return OM_ERR_OUT_OF_RANGE;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    uint32_t idx = (uint32_t)product_id * 2U + (side == OM_SIDE_BID ? 0U : 1U);
    khash_t(om_market_delta_map) *map = worker->deltas[idx];
    if (!map) {
        return 0;
    }
    size_t count = 0;
    for (khiter_t it = kh_begin(map); it != kh_end(map) && count < max; ++it) {
        if (!kh_exist(map, it)) {
            continue;
        }
        out[count].price = kh_key(map, it);
        out[count].delta = kh_val(map, it).delta;
        out[count].abs_qty = 0;  /* public path resolves qty via om_market_public_get_qty (O(1) ladder) */
        count++;
    }
    return (int)count;
}

int om_market_public_clear_deltas(OmMarketPublicWorker *worker,
                                  uint16_t product_id,
                                  uint16_t side) {
    if (!worker || !worker->deltas) {
        return OM_ERR_NULL_PARAM;
    }
    if (product_id >= worker->max_products) {
        return OM_ERR_OUT_OF_RANGE;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    uint32_t idx = (uint32_t)product_id * 2U + (side == OM_SIDE_BID ? 0U : 1U);
    khash_t(om_market_delta_map) *map = worker->deltas[idx];
    if (!map) {
        return 0;
    }
    kh_clear(om_market_delta_map, map);
    return 0;
}

/* ============================================================================
 * Copy Full Ladder (Walk Q1 from head - cache-friendly sequential access)
 * ============================================================================ */

int om_market_worker_copy_full(const OmMarketWorker *worker,
                               uint16_t org_id,
                               uint16_t product_id,
                               uint16_t side,
                               OmMarketDelta *out,
                               size_t max) {
    if (!worker || !out || max == 0) {
        return OM_ERR_NULL_PARAM;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return OM_ERR_NOT_SUBSCRIBED;
    }

    /* Build temp price->qty map from per-product order set — O(k) */
    khash_t(om_market_qty_map) *tmp = worker->scratch_qty_map;
    if (!tmp) {
        return OM_ERR_HASH_INIT;
    }
    kh_clear(om_market_qty_map, tmp);

    khash_t(om_market_order_set) *oset = worker->product_order_sets[product_id];
    for (khiter_t it = kh_begin(oset); it != kh_end(oset); ++it) {
        if (!kh_exist(oset, it)) continue;
        uint64_t order_id = kh_key(oset, it);
        khiter_t git = kh_get(om_market_order_map, worker->global_orders, order_id);
        if (git == kh_end(worker->global_orders)) continue;
        const OmMarketOrderState *state = &kh_val(worker->global_orders, git);
        if (!state->active || state->side != side) {
            continue;
        }
        uint64_t qty = om_market_compute_org_qty(worker, state, order_id, org_id);
        if (qty == 0) continue;

        int ret = 0;
        khiter_t qit = kh_get(om_market_qty_map, tmp, state->price);
        if (qit == kh_end(tmp)) {
            qit = kh_put(om_market_qty_map, tmp, state->price, &ret);
            if (ret < 0) {
                return OM_ERR_HASH_PUT;
            }
            kh_val(tmp, qit) = qty;
        } else {
            kh_val(tmp, qit) += qty;
        }
    }

    /* Walk product ladder Q1 in sorted order, lookup temp map */
    const OmMarketLadder *ladder = &worker->product_ladders[product_id];
    const OmMarketLevelSlab *slab = &worker->product_slab;
    bool is_bid = side == OM_SIDE_BID;
    uint32_t head = is_bid ? ladder->bid_head : ladder->ask_head;

    size_t count = 0;
    uint32_t slot_idx = head;
    while (slot_idx != OM_MARKET_SLOT_NULL && count < max) {
        const OmMarketLevelSlot *slot = &slab->slots[slot_idx];
        khiter_t qit = kh_get(om_market_qty_map, tmp, slot->price);
        if (qit != kh_end(tmp)) {
            uint64_t org_qty = kh_val(tmp, qit);
            if (org_qty > 0) {
                out[count].price = slot->price;
                out[count].delta = (int64_t)org_qty;
                out[count].abs_qty = org_qty;  /* full snapshot: abs qty == delta */
                count++;
            }
        }
        slot_idx = slot->q1_next;
    }

    return (int)count;
}

int om_market_public_copy_full(const OmMarketPublicWorker *worker,
                               uint16_t product_id,
                               uint16_t side,
                               OmMarketDelta *out,
                               size_t max) {
    if (!worker || !out) {
        return OM_ERR_NULL_PARAM;
    }
    if (max == 0) {
        return OM_ERR_INVALID_PARAM;
    }
    if (product_id >= worker->max_products) {
        return OM_ERR_OUT_OF_RANGE;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    const OmMarketLadder *ladder = &worker->ladders[product_id];
    const OmMarketLevelSlab *slab = &worker->slab;
    bool is_bid = side == OM_SIDE_BID;
    uint32_t head = is_bid ? ladder->bid_head : ladder->ask_head;

    /* Walk Q1 from head, copy up to max entries */
    size_t count = 0;
    uint32_t slot_idx = head;
    while (slot_idx != OM_MARKET_SLOT_NULL && count < max) {
        const OmMarketLevelSlot *slot = &slab->slots[slot_idx];
        out[count].price = slot->price;
        out[count].delta = (int64_t)slot->qty;
        out[count].abs_qty = slot->qty;  /* full snapshot: abs qty == delta */
        count++;
        slot_idx = slot->q1_next;
    }

    return (int)count;
}

/* ============================================================================
 * Dirty Flag Functions
 * ============================================================================ */

int om_market_worker_is_dirty(const OmMarketWorker *worker,
                              uint16_t org_id,
                              uint16_t product_id) {
    if (!worker || !worker->ladder_dirty) {
        return OM_ERR_NULL_PARAM;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    return worker->ladder_dirty[ladder_idx] ? 1 : 0;
}

int om_market_worker_clear_dirty(OmMarketWorker *worker,
                                 uint16_t org_id,
                                 uint16_t product_id) {
    if (!worker || !worker->ladder_dirty) {
        return OM_ERR_NULL_PARAM;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    return om_market_worker_clear_dirty_by_ladder(worker, ladder_idx);
}

int om_market_worker_clear_dirty_by_ladder(OmMarketWorker *worker,
                                           uint32_t ladder_idx) {
    if (!worker || !worker->ladder_dirty ||
        ladder_idx >= worker->subscription_count) {
        return OM_ERR_NULL_PARAM;
    }
    worker->ladder_dirty[ladder_idx] = 0;
    return 0;
}

int om_market_public_is_dirty(const OmMarketPublicWorker *worker, uint16_t product_id) {
    if (!worker || !worker->dirty) {
        return OM_ERR_NULL_PARAM;
    }
    if (product_id >= worker->max_products) {
        return OM_ERR_OUT_OF_RANGE;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    return worker->dirty[product_id] ? 1 : 0;
}

int om_market_public_clear_dirty(OmMarketPublicWorker *worker, uint16_t product_id) {
    if (!worker || !worker->dirty) {
        return OM_ERR_NULL_PARAM;
    }
    if (product_id >= worker->max_products) {
        return OM_ERR_OUT_OF_RANGE;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    worker->dirty[product_id] = 0;
    return 0;
}

/* ============================================================================
 * Dirty Work-List Iteration
 * ============================================================================ */

uint32_t om_market_worker_dirty_count(const OmMarketWorker *worker) {
    return (worker && worker->dirty_list) ? worker->dirty_count : 0U;
}

int om_market_worker_dirty_at(const OmMarketWorker *worker, uint32_t i,
                              uint16_t *org_id, uint16_t *product_id) {
    return om_market_worker_dirty_ladder_at(worker, i, NULL, org_id, product_id);
}

int om_market_worker_dirty_ladder_at(const OmMarketWorker *worker, uint32_t i,
                                     uint32_t *ladder_idx_out,
                                     uint16_t *org_id, uint16_t *product_id) {
    if (!worker || !worker->dirty_list || i >= worker->dirty_count) {
        return OM_ERR_OUT_OF_RANGE;
    }
    uint32_t ladder_idx = worker->dirty_list[i];
    if (ladder_idx >= worker->subscription_count) {
        return OM_ERR_OUT_OF_RANGE;
    }
    if (ladder_idx_out) {
        *ladder_idx_out = ladder_idx;
    }
    if (org_id) {
        *org_id = worker->ladder_org ? worker->ladder_org[ladder_idx] : 0U;
    }
    if (product_id) {
        *product_id = worker->ladder_product ? worker->ladder_product[ladder_idx] : 0U;
    }
    return 0;
}

void om_market_worker_compact_dirty(OmMarketWorker *worker) {
    if (!worker || !worker->dirty_list || !worker->ladder_dirty) {
        return;
    }
    uint32_t w = 0;
    for (uint32_t r = 0; r < worker->dirty_count; r++) {
        uint32_t ladder_idx = worker->dirty_list[r];
        if (ladder_idx < worker->subscription_count && worker->ladder_dirty[ladder_idx]) {
            worker->dirty_list[w++] = ladder_idx;
        }
    }
    worker->dirty_count = w;
}

uint32_t om_market_public_dirty_count(const OmMarketPublicWorker *worker) {
    return (worker && worker->dirty_list) ? worker->dirty_count : 0U;
}

int om_market_public_dirty_at(const OmMarketPublicWorker *worker, uint32_t i,
                              uint16_t *product_id) {
    if (!worker || !worker->dirty_list || i >= worker->dirty_count) {
        return OM_ERR_OUT_OF_RANGE;
    }
    if (product_id) {
        *product_id = worker->dirty_list[i];
    }
    return 0;
}

void om_market_public_compact_dirty(OmMarketPublicWorker *worker) {
    if (!worker || !worker->dirty_list || !worker->dirty) {
        return;
    }
    uint32_t w = 0;
    for (uint32_t r = 0; r < worker->dirty_count; r++) {
        uint16_t product_id = worker->dirty_list[r];
        if (product_id < worker->max_products && worker->dirty[product_id]) {
            worker->dirty_list[w++] = product_id;
        }
    }
    worker->dirty_count = w;
}
