#include "orderbook.h"
#include "om_wal.h"
#include "om_error.h"
#include <string.h>

int om_orderbook_init(OmOrderbookContext *ctx, const OmSlabConfig *config, struct OmWal *wal,
                      uint32_t max_products, uint32_t max_org, uint32_t hashmap_initial_cap)
{
    if (!ctx || !config) {
        return OM_ERR_NULL_PARAM;
    }

    if (max_products == 0 || max_org == 0) {
        return OM_ERR_INVALID_PARAM;
    }

    memset(ctx, 0, sizeof(OmOrderbookContext));
    ctx->max_products = max_products;
    ctx->max_org = max_org;

    ctx->products = calloc(max_products, sizeof(OmProductBook));
    if (!ctx->products) {
        return OM_ERR_PRODUCT_ALLOC;
    }

    ctx->org_heads = calloc((size_t)max_products * (size_t)max_org, sizeof(uint32_t));
    if (!ctx->org_heads) {
        free(ctx->products);
        ctx->products = NULL;
        return OM_ERR_ORG_ALLOC;
    }

    for (uint32_t i = 0; i < max_products * max_org; i++) {
        ctx->org_heads[i] = OM_SLOT_IDX_NULL;
    }

    int ret = om_slab_init(&ctx->slab, config);
    if (ret != 0) {
        free(ctx->org_heads);
        free(ctx->products);
        ctx->org_heads = NULL;
        ctx->products = NULL;
        return ret;
    }

    /* Initialize all product books to empty state */
    for (uint32_t i = 0; i < max_products; i++) {
        ctx->products[i].bid_head_q1 = OM_SLOT_IDX_NULL;
        ctx->products[i].ask_head_q1 = OM_SLOT_IDX_NULL;
    }

    /* Create order hashmap with initial capacity matching slab */
    uint32_t hash_cap = hashmap_initial_cap ? hashmap_initial_cap : config->total_slots;
    ctx->order_hashmap = om_hash_create(hash_cap);
    if (!ctx->order_hashmap) {
        om_slab_destroy(&ctx->slab);
        free(ctx->org_heads);
        free(ctx->products);
        ctx->org_heads = NULL;
        ctx->products = NULL;
        return OM_ERR_HASH_INIT;
    }

    ctx->next_slot_idx = 0;
    ctx->wal = wal;
    if (wal) {
        om_wal_set_slab(wal, &ctx->slab);
    }
    return 0;
}

void om_orderbook_destroy(OmOrderbookContext *ctx)
{
    /* Flush and sync WAL if enabled to ensure durability */
    if (ctx->wal) {
        om_wal_flush(ctx->wal);
        om_wal_fsync(ctx->wal);
    }

    if (ctx->order_hashmap) {
        om_hash_destroy(ctx->order_hashmap);
        ctx->order_hashmap = NULL;
    }
    om_slab_destroy(&ctx->slab);
    free(ctx->org_heads);
    free(ctx->products);
    ctx->org_heads = NULL;
    ctx->products = NULL;
}

/**
 * Find price level head order in Q1 for given product and price
 * Returns pointer to head order if found, NULL if not found
 * When not found, *insert_after is set to the node after which to insert
 * (NULL means insert at head, otherwise insert after this node)
 */
static OmSlabSlot *find_price_level_with_insertion_point(OmOrderbookContext *ctx, 
                                                          uint16_t product_id,
                                                          uint64_t price, 
                                                          bool is_bid,
                                                          OmSlabSlot **insert_after)
{
    OmProductBook *book = &ctx->products[product_id];
    uint32_t head_idx = is_bid ? book->bid_head_q1 : book->ask_head_q1;

    if (head_idx == OM_SLOT_IDX_NULL) {
        *insert_after = NULL;
        return NULL;
    }

    OmSlabSlot *head = om_slot_from_idx(&ctx->slab, head_idx);
    
    /* Check if we need to insert at head (new best price) */
    if (is_bid && price > head->price) {
        *insert_after = NULL;  /* Insert at head */
        return NULL;
    }
    if (!is_bid && price < head->price) {
        *insert_after = NULL;  /* Insert at head */
        return NULL;
    }

    /* Scan through Q1 price ladder */
    uint32_t curr_idx = head_idx;
    OmSlabSlot *curr = head;
    OmSlabSlot *prev = NULL;

    while (curr_idx != OM_SLOT_IDX_NULL) {
        if (curr->price == price) {
            *insert_after = NULL;  /* Not used when found */
            return curr;  /* Found exact price level */
        }

        /* For bids: scan descending, stop when price < target
         * For asks: scan ascending, stop when price > target
         */
        if (is_bid && curr->price < price) {
            *insert_after = prev;  /* Insert after previous */
            return NULL;
        }
        if (!is_bid && curr->price > price) {
            *insert_after = prev;  /* Insert after previous */
            return NULL;
        }

        prev = curr;
        curr_idx = curr->queue_nodes[OM_Q1_PRICE_LADDER].next_idx;
        if (curr_idx != OM_SLOT_IDX_NULL) {
            curr = om_slot_from_idx(&ctx->slab, curr_idx);
        }
    }

    /* Reached end - insert at tail */
    *insert_after = prev;
    return NULL;
}

/**
 * Insert order as new price level head into Q1 at given position
 * insert_after: NULL = insert at head, otherwise insert after this node
 */
static void insert_order_at(OmOrderbookContext *ctx, uint16_t product_id, bool is_bid,
                            OmSlabSlot *order, OmSlabSlot *insert_after)
{
    OmProductBook *book = &ctx->products[product_id];
    uint32_t *head_idx = is_bid ? &book->bid_head_q1 : &book->ask_head_q1;

    uint32_t order_idx = om_slot_get_idx(&ctx->slab, order);
    order->queue_nodes[OM_Q1_PRICE_LADDER].next_idx = OM_SLOT_IDX_NULL;
    order->queue_nodes[OM_Q1_PRICE_LADDER].prev_idx = OM_SLOT_IDX_NULL;

    /* Insert at the specified position */
    if (insert_after == NULL) {
        /* Insert at head */
        if (*head_idx != OM_SLOT_IDX_NULL) {
            OmSlabSlot *old_head = om_slot_from_idx(&ctx->slab, *head_idx);
            order->queue_nodes[OM_Q1_PRICE_LADDER].next_idx = *head_idx;
            old_head->queue_nodes[OM_Q1_PRICE_LADDER].prev_idx = order_idx;
        }
        *head_idx = order_idx;
    } else {
        /* Insert after the given node */
        om_queue_link_after(&ctx->slab, insert_after, order, OM_Q1_PRICE_LADDER);
    }
}

/**
 * Append order to tail of Q2 at given price level head order
 */
static void append_to_time_queue(OmOrderbookContext *ctx, OmSlabSlot *head,
                                 OmSlabSlot *order)
{
    /* Q2: Time FIFO at this price level
     * head->Q2.prev holds tail index (or NULL if only head)
     */
    uint32_t tail_idx = head->queue_nodes[OM_Q2_TIME_FIFO].prev_idx;
    OmSlabSlot *tail = head;

    if (tail_idx != OM_SLOT_IDX_NULL) {
        tail = om_slot_from_idx(&ctx->slab, tail_idx);
    }

    om_queue_link_after(&ctx->slab, tail, order, OM_Q2_TIME_FIFO);
    head->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = om_slot_get_idx(&ctx->slab, order);
}

/**
 * Remove price level head order from Q1
 */
static void remove_price_level(OmOrderbookContext *ctx, uint16_t product_id,
                               OmSlabSlot *level, bool is_bid)
{
    OmProductBook *book = &ctx->products[product_id];
    uint32_t *head_idx = is_bid ? &book->bid_head_q1 : &book->ask_head_q1;
    uint32_t level_idx = om_slot_get_idx(&ctx->slab, level);

    /* Update head if removing best level */
    if (*head_idx == level_idx) {
        *head_idx = level->queue_nodes[OM_Q1_PRICE_LADDER].next_idx;
    }

    /* Unlink from Q1 */
    om_queue_unlink(&ctx->slab, level, OM_Q1_PRICE_LADDER);

}

int om_orderbook_insert(OmOrderbookContext *ctx, uint16_t product_id,
                        OmSlabSlot *order)
{
    uint64_t price = order->price;
    bool is_bid = OM_IS_BID(order->flags);

    /* Find price level head order in Q1 */
    OmSlabSlot *insert_after = NULL;
    OmSlabSlot *head = find_price_level_with_insertion_point(ctx, product_id, price, is_bid, &insert_after);

    if (!head) {
        /* Insert order as new price level head */
        insert_order_at(ctx, product_id, is_bid, order, insert_after);
        order->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = OM_SLOT_IDX_NULL;
        order->queue_nodes[OM_Q2_TIME_FIFO].next_idx = OM_SLOT_IDX_NULL;
        head = order;
    } else {
        /* Append order to time queue at this price level (Q2) */
        append_to_time_queue(ctx, head, order);
    }

    /* Add order to org queue (Q3) per product */
    if (product_id < ctx->max_products && order->org < ctx->max_org) {
        uint32_t org_idx = product_id * ctx->max_org + order->org;
        uint32_t *head_idx = &ctx->org_heads[org_idx];
        if (*head_idx == OM_SLOT_IDX_NULL) {
            *head_idx = om_slot_get_idx(&ctx->slab, order);
            order->queue_nodes[OM_Q3_ORG_QUEUE].prev_idx = OM_SLOT_IDX_NULL;
            order->queue_nodes[OM_Q3_ORG_QUEUE].next_idx = OM_SLOT_IDX_NULL;
        } else {
            OmSlabSlot *head = om_slot_from_idx(&ctx->slab, *head_idx);
            om_queue_link_before(&ctx->slab, head, order, OM_Q3_ORG_QUEUE);
            *head_idx = om_slot_get_idx(&ctx->slab, order);
        }
    }

    /* Add order to hashmap for O(1) lookup by order_id */
    uint32_t slot_idx = om_slot_get_idx(&ctx->slab, order);
    OmOrderEntry entry = {
        .slot_idx = slot_idx,
        .product_id = product_id
    };
    om_hash_insert(ctx->order_hashmap, order->order_id, entry);

    /* Log to WAL if enabled */
    if (ctx->wal) {
        om_wal_insert(ctx->wal, order, product_id);
    }

    return 0;
}

bool om_orderbook_cancel(OmOrderbookContext *ctx, uint32_t order_id)
{
    /* Look up order entry from hashmap */
    OmOrderEntry *entry = om_hash_get(ctx->order_hashmap, order_id);
    if (!entry) {
        return false;  /* Order not found in hashmap */
    }

    uint32_t slot_idx = entry->slot_idx;
    uint16_t product_id = entry->product_id;

    /* Log to WAL if enabled (before removing from hashmap) */
    if (ctx->wal) {
        om_wal_cancel(ctx->wal, order_id, slot_idx, product_id);
    }

    OmSlabSlot *order = om_slot_from_idx(&ctx->slab, slot_idx);
    uint64_t price = order->price;
    bool is_bid = OM_IS_BID(order->flags);

    /* Find the price level head order - for cancel we only need lookup */
    OmSlabSlot *unused = NULL;
    OmSlabSlot *head = find_price_level_with_insertion_point(ctx, product_id, price, is_bid, &unused);
    if (!head) {
        return false;  /* Price level not found */
    }

    uint32_t head_idx = om_slot_get_idx(&ctx->slab, head);
    uint32_t next_idx = order->queue_nodes[OM_Q2_TIME_FIFO].next_idx;
    uint32_t prev_q2_idx = order->queue_nodes[OM_Q2_TIME_FIFO].prev_idx;

    if (order == head) {
        if (next_idx == OM_SLOT_IDX_NULL) {
            /* No more orders at this price - remove price level */
            remove_price_level(ctx, product_id, head, is_bid);
        } else {
            OmProductBook *book = &ctx->products[product_id];
            uint32_t *book_head = is_bid ? &book->bid_head_q1 : &book->ask_head_q1;
            OmSlabSlot *next = om_slot_from_idx(&ctx->slab, next_idx);

            /* Promote next to head: update Q2 head tail pointer */
            next->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = head->queue_nodes[OM_Q2_TIME_FIFO].prev_idx;
            if (next->queue_nodes[OM_Q2_TIME_FIFO].next_idx == OM_SLOT_IDX_NULL) {
                /* Only one order remains at this price */
                next->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = OM_SLOT_IDX_NULL;
            }

            /* Fix Q2 prev pointer of the following node */
            if (next->queue_nodes[OM_Q2_TIME_FIFO].next_idx != OM_SLOT_IDX_NULL) {
                OmSlabSlot *after = om_slot_from_idx(&ctx->slab,
                                                     next->queue_nodes[OM_Q2_TIME_FIFO].next_idx);
                if (after) {
                    after->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = om_slot_get_idx(&ctx->slab, next);
                }
            }

            uint32_t prev_q1 = head->queue_nodes[OM_Q1_PRICE_LADDER].prev_idx;
            uint32_t next_q1 = head->queue_nodes[OM_Q1_PRICE_LADDER].next_idx;

            next->queue_nodes[OM_Q1_PRICE_LADDER].prev_idx = prev_q1;
            next->queue_nodes[OM_Q1_PRICE_LADDER].next_idx = next_q1;

            if (*book_head == head_idx) {
                *book_head = next_idx;
            }

            if (prev_q1 != OM_SLOT_IDX_NULL) {
                OmSlabSlot *prev = om_slot_from_idx(&ctx->slab, prev_q1);
                if (prev) {
                    prev->queue_nodes[OM_Q1_PRICE_LADDER].next_idx = next_idx;
                }
            }

            if (next_q1 != OM_SLOT_IDX_NULL) {
                OmSlabSlot *q1_next = om_slot_from_idx(&ctx->slab, next_q1);
                if (q1_next) {
                    q1_next->queue_nodes[OM_Q1_PRICE_LADDER].prev_idx = next_idx;
                }
            }

            head->queue_nodes[OM_Q1_PRICE_LADDER].next_idx = OM_SLOT_IDX_NULL;
            head->queue_nodes[OM_Q1_PRICE_LADDER].prev_idx = OM_SLOT_IDX_NULL;
            head->queue_nodes[OM_Q2_TIME_FIFO].next_idx = OM_SLOT_IDX_NULL;
            head->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = OM_SLOT_IDX_NULL;
        }
    } else {
        /* Remove non-head order from time queue Q2 */
        om_queue_unlink(&ctx->slab, order, OM_Q2_TIME_FIFO);

        /* Maintain Q2 tail pointer on head */
        if (next_idx == OM_SLOT_IDX_NULL) {
            if (prev_q2_idx == head_idx) {
                head->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = OM_SLOT_IDX_NULL;
            } else {
                head->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = prev_q2_idx;
            }
        }
    }

    /* Remove from org queue Q3 */
    if (product_id < ctx->max_products && order->org < ctx->max_org) {
        uint32_t org_idx = product_id * ctx->max_org + order->org;
        uint32_t *head_idx = &ctx->org_heads[org_idx];
        if (*head_idx == slot_idx) {
            *head_idx = order->queue_nodes[OM_Q3_ORG_QUEUE].next_idx;
        }
    }
    om_queue_unlink(&ctx->slab, order, OM_Q3_ORG_QUEUE);

    /* Remove from hashmap */
    om_hash_remove(ctx->order_hashmap, order_id);

    /* Free the order slot */
    om_slab_free(&ctx->slab, order);

    return true;
}

uint64_t om_orderbook_get_best_bid(const OmOrderbookContext *ctx, uint16_t product_id)
{
    uint32_t head_idx = ctx->products[product_id].bid_head_q1;
    if (head_idx == OM_SLOT_IDX_NULL) {
        return 0;
    }

    OmSlabSlot *head = om_slot_from_idx((OmDualSlab *)&ctx->slab, head_idx);
    return head->price;
}

uint64_t om_orderbook_get_best_ask(const OmOrderbookContext *ctx, uint16_t product_id)
{
    uint32_t head_idx = ctx->products[product_id].ask_head_q1;
    if (head_idx == OM_SLOT_IDX_NULL) {
        return 0;
    }

    OmSlabSlot *head = om_slot_from_idx((OmDualSlab *)&ctx->slab, head_idx);
    return head->price;
}

uint64_t om_orderbook_get_volume_at_price(const OmOrderbookContext *ctx,
                                           uint16_t product_id, uint64_t price,
                                           bool is_bid)
{
    OmSlabSlot *unused = NULL;
    OmSlabSlot *level = find_price_level_with_insertion_point((OmOrderbookContext *)ctx, product_id, price, is_bid, &unused);
    if (!level) {
        return 0;
    }

    /* Sum volume_remain of all orders in Q2 at this price */
    uint64_t total_volume = 0;
    uint32_t order_idx = om_slot_get_idx(&ctx->slab, level);

    while (order_idx != OM_SLOT_IDX_NULL) {
        OmSlabSlot *order = om_slot_from_idx((OmDualSlab *)&ctx->slab, order_idx);
        total_volume += order->volume_remain;
        order_idx = order->queue_nodes[OM_Q2_TIME_FIFO].next_idx;
    }

    return total_volume;
}

OmSlabSlot *om_orderbook_get_slot_by_id(OmOrderbookContext *ctx, uint32_t order_id)
{
    /* Look up order entry from hashmap */
    OmOrderEntry *entry = om_hash_get(ctx->order_hashmap, order_id);
    if (!entry) {
        return NULL;  /* Order not found or not active */
    }

    return om_slot_from_idx(&ctx->slab, entry->slot_idx);
}

OmSlabSlot *om_orderbook_get_best_head(const OmOrderbookContext *ctx,
                                       uint16_t product_id, bool is_bid)
{
    uint32_t head_idx = is_bid ? ctx->products[product_id].bid_head_q1
                               : ctx->products[product_id].ask_head_q1;
    if (head_idx == OM_SLOT_IDX_NULL) {
        return NULL;
    }

    return om_slot_from_idx((OmDualSlab *)&ctx->slab, head_idx);
}

bool om_orderbook_remove_slot(OmOrderbookContext *ctx, uint16_t product_id, OmSlabSlot *order)
{
    uint64_t price = order->price;
    bool is_bid = OM_IS_BID(order->flags);

    OmSlabSlot *unused = NULL;
    OmSlabSlot *head = find_price_level_with_insertion_point(ctx, product_id, price, is_bid, &unused);
    if (!head) {
        return false;
    }

    uint32_t head_idx = om_slot_get_idx(&ctx->slab, head);
    uint32_t next_idx = order->queue_nodes[OM_Q2_TIME_FIFO].next_idx;
    uint32_t prev_q2_idx = order->queue_nodes[OM_Q2_TIME_FIFO].prev_idx;

    if (order == head) {
        if (next_idx == OM_SLOT_IDX_NULL) {
            remove_price_level(ctx, product_id, head, is_bid);
        } else {
            OmProductBook *book = &ctx->products[product_id];
            uint32_t *book_head = is_bid ? &book->bid_head_q1 : &book->ask_head_q1;
            OmSlabSlot *next = om_slot_from_idx(&ctx->slab, next_idx);

            next->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = head->queue_nodes[OM_Q2_TIME_FIFO].prev_idx;
            if (next->queue_nodes[OM_Q2_TIME_FIFO].next_idx == OM_SLOT_IDX_NULL) {
                next->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = OM_SLOT_IDX_NULL;
            }

            if (next->queue_nodes[OM_Q2_TIME_FIFO].next_idx != OM_SLOT_IDX_NULL) {
                OmSlabSlot *after = om_slot_from_idx(&ctx->slab,
                                                     next->queue_nodes[OM_Q2_TIME_FIFO].next_idx);
                if (after) {
                    after->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = om_slot_get_idx(&ctx->slab, next);
                }
            }

            uint32_t prev_q1 = head->queue_nodes[OM_Q1_PRICE_LADDER].prev_idx;
            uint32_t next_q1 = head->queue_nodes[OM_Q1_PRICE_LADDER].next_idx;

            next->queue_nodes[OM_Q1_PRICE_LADDER].prev_idx = prev_q1;
            next->queue_nodes[OM_Q1_PRICE_LADDER].next_idx = next_q1;

            if (*book_head == head_idx) {
                *book_head = next_idx;
            }

            if (prev_q1 != OM_SLOT_IDX_NULL) {
                OmSlabSlot *prev = om_slot_from_idx(&ctx->slab, prev_q1);
                if (prev) {
                    prev->queue_nodes[OM_Q1_PRICE_LADDER].next_idx = next_idx;
                }
            }

            if (next_q1 != OM_SLOT_IDX_NULL) {
                OmSlabSlot *q1_next = om_slot_from_idx(&ctx->slab, next_q1);
                if (q1_next) {
                    q1_next->queue_nodes[OM_Q1_PRICE_LADDER].prev_idx = next_idx;
                }
            }

            head->queue_nodes[OM_Q1_PRICE_LADDER].next_idx = OM_SLOT_IDX_NULL;
            head->queue_nodes[OM_Q1_PRICE_LADDER].prev_idx = OM_SLOT_IDX_NULL;
            head->queue_nodes[OM_Q2_TIME_FIFO].next_idx = OM_SLOT_IDX_NULL;
            head->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = OM_SLOT_IDX_NULL;
        }
    } else {
        om_queue_unlink(&ctx->slab, order, OM_Q2_TIME_FIFO);
        if (next_idx == OM_SLOT_IDX_NULL) {
            if (prev_q2_idx == head_idx) {
                head->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = OM_SLOT_IDX_NULL;
            } else {
                head->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = prev_q2_idx;
            }
        }
    }

    om_queue_unlink(&ctx->slab, order, OM_Q3_ORG_QUEUE);
    om_hash_remove(ctx->order_hashmap, order->order_id);
    om_slab_free(&ctx->slab, order);

    return true;
}

bool om_orderbook_unlink_slot(OmOrderbookContext *ctx, uint16_t product_id, OmSlabSlot *order)
{
    uint64_t price = order->price;
    bool is_bid = OM_IS_BID(order->flags);

    OmSlabSlot *unused = NULL;
    OmSlabSlot *head = find_price_level_with_insertion_point(ctx, product_id, price, is_bid, &unused);
    if (!head) {
        return false;
    }

    uint32_t head_idx = om_slot_get_idx(&ctx->slab, head);
    uint32_t next_idx = order->queue_nodes[OM_Q2_TIME_FIFO].next_idx;
    uint32_t prev_q2_idx = order->queue_nodes[OM_Q2_TIME_FIFO].prev_idx;

    if (order == head) {
        if (next_idx == OM_SLOT_IDX_NULL) {
            remove_price_level(ctx, product_id, head, is_bid);
        } else {
            OmProductBook *book = &ctx->products[product_id];
            uint32_t *book_head = is_bid ? &book->bid_head_q1 : &book->ask_head_q1;
            OmSlabSlot *next = om_slot_from_idx(&ctx->slab, next_idx);

            next->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = head->queue_nodes[OM_Q2_TIME_FIFO].prev_idx;
            if (next->queue_nodes[OM_Q2_TIME_FIFO].next_idx == OM_SLOT_IDX_NULL) {
                next->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = OM_SLOT_IDX_NULL;
            }

            if (next->queue_nodes[OM_Q2_TIME_FIFO].next_idx != OM_SLOT_IDX_NULL) {
                OmSlabSlot *after = om_slot_from_idx(&ctx->slab,
                                                     next->queue_nodes[OM_Q2_TIME_FIFO].next_idx);
                if (after) {
                    after->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = om_slot_get_idx(&ctx->slab, next);
                }
            }

            uint32_t prev_q1 = head->queue_nodes[OM_Q1_PRICE_LADDER].prev_idx;
            uint32_t next_q1 = head->queue_nodes[OM_Q1_PRICE_LADDER].next_idx;

            next->queue_nodes[OM_Q1_PRICE_LADDER].prev_idx = prev_q1;
            next->queue_nodes[OM_Q1_PRICE_LADDER].next_idx = next_q1;

            if (*book_head == head_idx) {
                *book_head = next_idx;
            }

            if (prev_q1 != OM_SLOT_IDX_NULL) {
                OmSlabSlot *prev = om_slot_from_idx(&ctx->slab, prev_q1);
                if (prev) {
                    prev->queue_nodes[OM_Q1_PRICE_LADDER].next_idx = next_idx;
                }
            }

            if (next_q1 != OM_SLOT_IDX_NULL) {
                OmSlabSlot *q1_next = om_slot_from_idx(&ctx->slab, next_q1);
                if (q1_next) {
                    q1_next->queue_nodes[OM_Q1_PRICE_LADDER].prev_idx = next_idx;
                }
            }

            head->queue_nodes[OM_Q1_PRICE_LADDER].next_idx = OM_SLOT_IDX_NULL;
            head->queue_nodes[OM_Q1_PRICE_LADDER].prev_idx = OM_SLOT_IDX_NULL;
            head->queue_nodes[OM_Q2_TIME_FIFO].next_idx = OM_SLOT_IDX_NULL;
            head->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = OM_SLOT_IDX_NULL;
        }
    } else {
        om_queue_unlink(&ctx->slab, order, OM_Q2_TIME_FIFO);
        if (next_idx == OM_SLOT_IDX_NULL) {
            if (prev_q2_idx == head_idx) {
                head->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = OM_SLOT_IDX_NULL;
            } else {
                head->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = prev_q2_idx;
            }
        }
    }

    if (product_id < ctx->max_products && order->org < ctx->max_org) {
        uint32_t org_idx = product_id * ctx->max_org + order->org;
        uint32_t *head_idx = &ctx->org_heads[org_idx];
        uint32_t slot_idx = om_slot_get_idx(&ctx->slab, order);
        if (*head_idx == slot_idx) {
            *head_idx = order->queue_nodes[OM_Q3_ORG_QUEUE].next_idx;
        }
    }
    om_queue_unlink(&ctx->slab, order, OM_Q3_ORG_QUEUE);
    return true;
}

uint32_t om_orderbook_cancel_org_product(OmOrderbookContext *ctx, uint16_t product_id, uint16_t org_id)
{
    if (!ctx || product_id >= ctx->max_products || org_id >= ctx->max_org) {
        return 0;
    }

    uint32_t org_idx = product_id * ctx->max_org + org_id;
    uint32_t head_idx = ctx->org_heads[org_idx];
    uint32_t cancelled = 0;

    while (head_idx != OM_SLOT_IDX_NULL) {
        OmSlabSlot *order = om_slot_from_idx(&ctx->slab, head_idx);
        if (!order) {
            break;
        }
        uint32_t next_idx = order->queue_nodes[OM_Q3_ORG_QUEUE].next_idx;
        if (om_orderbook_cancel(ctx, order->order_id)) {
            cancelled++;
        }
        head_idx = next_idx;
    }

    return cancelled;
}

uint32_t om_orderbook_cancel_org_all(OmOrderbookContext *ctx, uint16_t org_id)
{
    if (!ctx || org_id >= ctx->max_org) {
        return 0;
    }

    uint32_t cancelled = 0;
    for (uint32_t product_id = 0; product_id < ctx->max_products; product_id++) {
        cancelled += om_orderbook_cancel_org_product(ctx, (uint16_t)product_id, org_id);
    }

    return cancelled;
}

uint32_t om_orderbook_cancel_product_side(OmOrderbookContext *ctx, uint16_t product_id, bool is_bid)
{
    if (!ctx || product_id >= ctx->max_products) {
        return 0;
    }

    uint32_t cancelled = 0;
    uint32_t level_idx = is_bid ? ctx->products[product_id].bid_head_q1
                                : ctx->products[product_id].ask_head_q1;

    while (level_idx != OM_SLOT_IDX_NULL) {
        OmSlabSlot *level = om_slot_from_idx(&ctx->slab, level_idx);
        if (!level) {
            break;
        }
        uint32_t next_level_idx = level->queue_nodes[OM_Q1_PRICE_LADDER].next_idx;

        uint32_t order_idx = level_idx;
        while (order_idx != OM_SLOT_IDX_NULL) {
            OmSlabSlot *order = om_slot_from_idx(&ctx->slab, order_idx);
            if (!order) {
                break;
            }
            uint32_t next_order_idx = order->queue_nodes[OM_Q2_TIME_FIFO].next_idx;
            if (om_orderbook_cancel(ctx, order->order_id)) {
                cancelled++;
            }
            order_idx = next_order_idx;
        }

        level_idx = next_level_idx;
    }

    return cancelled;
}

uint32_t om_orderbook_cancel_product(OmOrderbookContext *ctx, uint16_t product_id)
{
    if (!ctx || product_id >= ctx->max_products) {
        return 0;
    }

    uint32_t cancelled = 0;
    cancelled += om_orderbook_cancel_product_side(ctx, product_id, true);
    cancelled += om_orderbook_cancel_product_side(ctx, product_id, false);
    return cancelled;
}

bool om_orderbook_price_level_exists(const OmOrderbookContext *ctx,
                                      uint16_t product_id, uint64_t price,
                                      bool is_bid)
{
    OmSlabSlot *unused = NULL;
    OmSlabSlot *level = find_price_level_with_insertion_point((OmOrderbookContext *)ctx, product_id, price, is_bid, &unused);
    return (level != NULL);
}

uint32_t om_orderbook_get_price_level_count(const OmOrderbookContext *ctx,
                                             uint16_t product_id, bool is_bid)
{
    uint32_t head_idx = is_bid ? ctx->products[product_id].bid_head_q1
                               : ctx->products[product_id].ask_head_q1;

    if (head_idx == OM_SLOT_IDX_NULL) {
        return 0;
    }

    uint32_t count = 0;
    uint32_t curr_idx = head_idx;

    while (curr_idx != OM_SLOT_IDX_NULL) {
        count++;
        OmSlabSlot *curr = om_slot_from_idx((OmDualSlab *)&ctx->slab, curr_idx);
        curr_idx = curr->queue_nodes[OM_Q1_PRICE_LADDER].next_idx;
    }

    return count;
}

/* ============================================================================
 * WAL RECOVERY - Reconstruct orderbook from WAL file
 * ============================================================================ */

int om_orderbook_recover_from_wal(OmOrderbookContext *ctx, 
                                   const char *wal_filename,
                                   OmWalReplayStats *stats)
{
    if (!ctx || !wal_filename) {
        return OM_ERR_NULL_PARAM;
    }

    /* Initialize stats if provided */
    if (stats) {
        memset(stats, 0, sizeof(OmWalReplayStats));
    }

    /* Initialize WAL replay iterator */
    OmWalReplay replay;
    OmWalConfig replay_config = {
        .filename = wal_filename,
        .buffer_size = 0,
        .sync_interval_ms = 0,
        .use_direct_io = false,
        .disable_crc32 = ctx->wal ? !ctx->wal->config.enable_crc32 : false,
        .user_data_size = ctx->slab.config.user_data_size,
        .aux_data_size = ctx->slab.config.aux_data_size
    };

    if (om_wal_replay_init_with_config(&replay, wal_filename, &replay_config) != 0) {
        return OM_ERR_WAL_OPEN;
    }

    /* Replay all records */
    OmWalType type;
    void *data;
    uint64_t sequence;
    size_t data_len;

    int replay_status = 0;
    while ((replay_status = om_wal_replay_next(&replay, &type, &data, &sequence, &data_len)) == 1) {
        switch (type) {
            case OM_WAL_INSERT: {
                if (data_len < sizeof(OmWalInsert)) {
                    continue;
                }
                
                OmWalInsert rec;
                memcpy(&rec, data, sizeof(OmWalInsert));
                
                OmSlabSlot *slot = om_slab_alloc(&ctx->slab);
                if (!slot) {
                    om_wal_replay_close(&replay);
                    return OM_ERR_SLAB_FULL;
                }
                
                slot->order_id = rec.order_id;
                slot->price = rec.price;
                slot->volume = rec.volume;
                slot->volume_remain = rec.vol_remain;
                slot->org = rec.org;
                slot->flags = rec.flags;
                
                if (rec.user_data_size > 0 || rec.aux_data_size > 0) {
                    uint8_t *user_data_src = (uint8_t *)data + sizeof(OmWalInsert);
                    uint8_t *aux_data_src = user_data_src + rec.user_data_size;
                    
                    if (rec.user_data_size > 0) {
                        void *user_data = om_slot_get_data(slot);
                        memcpy(user_data, user_data_src, rec.user_data_size);
                    }
                    
                    if (rec.aux_data_size > 0) {
                        void *aux_data = om_slot_get_aux_data(&ctx->slab, slot);
                        memcpy(aux_data, aux_data_src, rec.aux_data_size);
                    }
                }
                
                if (om_orderbook_insert(ctx, rec.product_id, slot) != 0) {
                    om_slab_free(&ctx->slab, slot);
                    om_wal_replay_close(&replay);
                    return OM_ERR_RECOVERY_FAILED;
                }
                
                if (stats) {
                    stats->records_insert++;
                    stats->last_sequence = sequence;
                }
                break;
            }
            
            case OM_WAL_CANCEL: {
                if (data_len != sizeof(OmWalCancel)) {
                    continue;
                }
                OmWalCancel rec;
                memcpy(&rec, data, sizeof(OmWalCancel));
                
                om_orderbook_cancel(ctx, rec.order_id);
                
                if (stats) {
                    stats->records_cancel++;
                    stats->last_sequence = sequence;
                }
                break;
            }
            
            case OM_WAL_MATCH: {
                if (data_len != sizeof(OmWalMatch)) {
                    continue;
                }
                OmWalMatch rec;
                memcpy(&rec, data, sizeof(OmWalMatch));
                
                OmOrderEntry *entry = om_hash_get(ctx->order_hashmap, rec.maker_id);
                if (entry) {
                    OmSlabSlot *slot = om_slot_from_idx(&ctx->slab, entry->slot_idx);
                    if (slot && slot->volume_remain >= rec.volume) {
                        slot->volume_remain -= rec.volume;
                        
                        if (slot->volume_remain == 0) {
                            om_orderbook_cancel(ctx, rec.maker_id);
            }
        }
    }
                
                if (stats) {
                    stats->records_match++;
                    stats->last_sequence = sequence;
                }
                break;
            }

            case OM_WAL_DEACTIVATE: {
                if (data_len != sizeof(OmWalDeactivate)) {
                    continue;
                }
                OmWalDeactivate rec;
                memcpy(&rec, data, sizeof(OmWalDeactivate));

                OmOrderEntry *entry = om_hash_get(ctx->order_hashmap, rec.order_id);
                if (entry) {
                    OmSlabSlot *slot = om_slot_from_idx(&ctx->slab, entry->slot_idx);
                    if (slot) {
                        om_orderbook_unlink_slot(ctx, entry->product_id, slot);
                        slot->flags = OM_SET_STATUS(slot->flags, OM_STATUS_DEACTIVATED);
                    }
                }

                if (stats) {
                    stats->records_other++;
                    stats->last_sequence = sequence;
                }
                break;
            }

            case OM_WAL_ACTIVATE: {
                if (data_len != sizeof(OmWalActivate)) {
                    continue;
                }
                OmWalActivate rec;
                memcpy(&rec, data, sizeof(OmWalActivate));

                OmOrderEntry *entry = om_hash_get(ctx->order_hashmap, rec.order_id);
                if (entry) {
                    OmSlabSlot *slot = om_slot_from_idx(&ctx->slab, entry->slot_idx);
                    if (slot && (slot->flags & OM_STATUS_MASK) == OM_STATUS_DEACTIVATED) {
                        slot->flags = OM_SET_STATUS(slot->flags, OM_STATUS_NEW);
                        om_orderbook_insert(ctx, entry->product_id, slot);
                    }
                }

                if (stats) {
                    stats->records_other++;
                    stats->last_sequence = sequence;
                }
                break;
            }
            
            default:
                if (stats) {
                    stats->records_other++;
                    stats->last_sequence = sequence;
                }
                break;
        }
        
        if (stats) {
            stats->bytes_processed += sizeof(OmWalHeader) + data_len;
        }
    }

    if (replay_status < 0) {
        om_wal_replay_close(&replay);
        /* Propagate the specific failure (e.g. OM_ERR_WAL_TRUNCATED for a torn
         * tail vs OM_ERR_WAL_CRC_MISMATCH for corruption) so callers can treat a
         * benign crash-truncated tail differently from real corruption. Still a
         * negative value, so existing sign-based success checks are unaffected. */
        return replay_status;
    }

    om_wal_replay_close(&replay);
    return 0;
}
