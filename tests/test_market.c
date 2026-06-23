#include <check.h>
#include <pthread.h>
#include <stdint.h>
#include "openmarket/om_worker.h"
#include "openmarket/om_market.h"
#include "openmatch/om_error.h"

START_TEST(test_market_struct_sizes) {
    /* Verify OmMarketLevelSlot is exactly 32 bytes (2 per cache line) */
    ck_assert_uint_eq(sizeof(OmMarketLevelSlot), 32);

    /* Verify OM_MARKET_SLOT_NULL is UINT32_MAX */
    ck_assert_uint_eq(OM_MARKET_SLOT_NULL, UINT32_MAX);
}
END_TEST

START_TEST(test_market_ring_basic) {
    OmMarketRing ring;
    OmMarketRingConfig config = {
        .capacity = 8,
        .consumer_count = 2
    };

    ck_assert_int_eq(om_market_ring_init(&ring, &config), 0);
    ck_assert_int_eq(om_market_ring_register_consumer(&ring, 0), 0);
    ck_assert_int_eq(om_market_ring_register_consumer(&ring, 1), 0);

    uint64_t a = 1;
    uint64_t b = 2;
    void *out = NULL;

    ck_assert_int_eq(om_market_ring_enqueue(&ring, &a), 0);
    ck_assert_int_eq(om_market_ring_enqueue(&ring, &b), 0);

    ck_assert_int_eq(om_market_ring_dequeue(&ring, 0, &out), 1);
    ck_assert_ptr_eq(out, &a);
    ck_assert_int_eq(om_market_ring_dequeue(&ring, 1, &out), 1);
    ck_assert_ptr_eq(out, &a);

    ck_assert_int_eq(om_market_ring_dequeue(&ring, 0, &out), 1);
    ck_assert_ptr_eq(out, &b);
    ck_assert_int_eq(om_market_ring_dequeue(&ring, 1, &out), 1);
    ck_assert_ptr_eq(out, &b);

    om_market_ring_destroy(&ring);
}
END_TEST

typedef struct OmMarketWaitCtx {
    OmMarketRing *ring;
    uint32_t consumer_index;
    size_t min_batch;
    int result;
} OmMarketWaitCtx;

static void *om_market_wait_thread(void *arg) {
    OmMarketWaitCtx *ctx = (OmMarketWaitCtx *)arg;
    ctx->result = om_market_ring_wait(ctx->ring, ctx->consumer_index, ctx->min_batch);
    return NULL;
}

START_TEST(test_market_ring_wait_notify) {
    OmMarketRing ring;
    OmMarketRingConfig config = {
        .capacity = 8,
        .consumer_count = 1,
        .notify_batch = 2
    };

    ck_assert_int_eq(om_market_ring_init(&ring, &config), 0);
    ck_assert_int_eq(om_market_ring_register_consumer(&ring, 0), 0);

    OmMarketWaitCtx ctx = {
        .ring = &ring,
        .consumer_index = 0,
        .min_batch = 2,
        .result = -1
    };
    pthread_t thread;
    ck_assert_int_eq(pthread_create(&thread, NULL, om_market_wait_thread, &ctx), 0);

    uint64_t a = 101;
    uint64_t b = 202;
    ck_assert_int_eq(om_market_ring_enqueue(&ring, &a), 0);
    ck_assert_int_eq(om_market_ring_enqueue(&ring, &b), 0);

    ck_assert_int_eq(pthread_join(thread, NULL), 0);
    ck_assert_int_eq(ctx.result, 0);

    void *out[2] = {0};
    int count = om_market_ring_dequeue_batch(&ring, 0, out, 2);
    ck_assert_int_eq(count, 2);
    ck_assert_ptr_eq(out[0], &a);
    ck_assert_ptr_eq(out[1], &b);

    om_market_ring_destroy(&ring);
}
END_TEST

START_TEST(test_market_ring_batch) {
    OmMarketRing ring;
    OmMarketRingConfig config = {
        .capacity = 8,
        .consumer_count = 1
    };

    ck_assert_int_eq(om_market_ring_init(&ring, &config), 0);
    ck_assert_int_eq(om_market_ring_register_consumer(&ring, 0), 0);

    uint64_t a = 11;
    uint64_t b = 22;
    uint64_t c = 33;
    void *out[4] = {0};

    ck_assert_int_eq(om_market_ring_enqueue(&ring, &a), 0);
    ck_assert_int_eq(om_market_ring_enqueue(&ring, &b), 0);
    ck_assert_int_eq(om_market_ring_enqueue(&ring, &c), 0);

    int count = om_market_ring_dequeue_batch(&ring, 0, out, 4);
    ck_assert_int_eq(count, 3);
    ck_assert_ptr_eq(out[0], &a);
    ck_assert_ptr_eq(out[1], &b);
    ck_assert_ptr_eq(out[2], &c);

    om_market_ring_destroy(&ring);
}
END_TEST

static uint64_t test_marketable(const OmWalInsert *rec, uint16_t viewer_org, void *ctx) {
    (void)ctx;
    return rec->org == viewer_org ? 0 : rec->vol_remain;
}

/* Multi-org visibility: org 1's orders are visible to both org 2 and org 3 */
static uint64_t test_multi_org_marketable(const OmWalInsert *rec, uint16_t viewer_org, void *ctx) {
    (void)ctx;
    if (rec->org == 1 && (viewer_org == 2 || viewer_org == 3)) {
        return rec->vol_remain;
    }
    return 0;
}

START_TEST(test_market_worker_dealable) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) {
        org_to_worker[i] = 0;
    }
    OmMarketSubscription subs[2] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 2, .product_id = 0}
    };
    OmMarketConfig cfg = {
        .max_products = 16,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = org_to_worker,
        .subs = subs,
        .sub_count = 2,
        .expected_orders_per_worker = 4,
        .expected_subscribers_per_product = 1,
        .expected_price_levels = 4,
        .top_levels = 1,
        .dealable = test_marketable,
        .dealable_ctx = NULL
    };

    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketWorker *worker = om_market_worker(&market, 0);
    ck_assert_ptr_nonnull(worker);

    OmWalInsert insert = {
        .order_id = 100,
        .price = 10,
        .volume = 50,
        .vol_remain = 50,
        .org = 1,
        .flags = OM_SIDE_BID,
        .product_id = 0
    };

    ck_assert_int_eq(om_market_worker_process(worker, OM_WAL_INSERT, &insert), 0);
    ck_assert_int_eq(om_market_public_process(&market.public_workers[0], OM_WAL_INSERT, &insert), 0);

    uint64_t qty = 0;
    ck_assert_int_ne(om_market_worker_get_qty(worker, 1, 0, OM_SIDE_BID, 10, &qty), 0);
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 10, &qty), 0);
    ck_assert_uint_eq(qty, 50);
    ck_assert_int_eq(om_market_public_get_qty(&market.public_workers[0], 0, OM_SIDE_BID, 10, &qty), 0);
    ck_assert_uint_eq(qty, 50);

    OmWalInsert insert2 = {
        .order_id = 101,
        .price = 9,
        .volume = 30,
        .vol_remain = 30,
        .org = 2,
        .flags = OM_SIDE_BID,
        .product_id = 0
    };
    ck_assert_int_eq(om_market_worker_process(worker, OM_WAL_INSERT, &insert2), 0);
    ck_assert_int_eq(om_market_public_process(&market.public_workers[0], OM_WAL_INSERT, &insert2), 0);
    /* All prices are now tracked in the ladder (not just top-N) */
    ck_assert_int_eq(om_market_public_get_qty(&market.public_workers[0], 0, OM_SIDE_BID, 9, &qty), 0);
    ck_assert_uint_eq(qty, 30);
    /* Org 2 can't see its own order due to dealable callback */
    ck_assert_int_ne(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 9, &qty), 0);
    ck_assert_int_eq(om_market_worker_is_dirty(worker, 2, 0), 1);
    ck_assert_int_eq(om_market_public_is_dirty(&market.public_workers[0], 0), 1);
    ck_assert_int_eq(om_market_worker_clear_dirty(worker, 2, 0), 0);
    ck_assert_int_eq(om_market_public_clear_dirty(&market.public_workers[0], 0), 0);
    ck_assert_int_eq(om_market_worker_is_dirty(worker, 2, 0), 0);
    ck_assert_int_eq(om_market_public_is_dirty(&market.public_workers[0], 0), 0);
    OmMarketDelta deltas[4];
    ck_assert_int_eq(om_market_public_delta_count(&market.public_workers[0], 0, OM_SIDE_BID), 2);
    ck_assert_int_eq(om_market_public_copy_deltas(&market.public_workers[0], 0, OM_SIDE_BID, deltas, 4), 2);
    bool found_10 = false;
    for (int i = 0; i < 2; i++) {
        if (deltas[i].price == 10 && deltas[i].delta == 50) {
            found_10 = true;
        }
    }
    ck_assert(found_10);
    /* With dynamic ladder, all prices are tracked. Use max=1 to get top-1 only */
    ck_assert_int_eq(om_market_public_copy_full(&market.public_workers[0], 0, OM_SIDE_BID, deltas, 1), 1);
    ck_assert_uint_eq(deltas[0].price, 10);
    ck_assert_int_eq(deltas[0].delta, 50);
    ck_assert_int_eq(om_market_public_clear_deltas(&market.public_workers[0], 0, OM_SIDE_BID), 0);
    ck_assert_int_eq(om_market_public_delta_count(&market.public_workers[0], 0, OM_SIDE_BID), 0);
    ck_assert_int_eq(om_market_worker_is_subscribed(worker, 2, 0), 1);
    ck_assert_int_eq(om_market_worker_is_subscribed(worker, 2, 1), 0);

    om_market_destroy(&market);
}
END_TEST

START_TEST(test_market_publish_combos) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) {
        org_to_worker[i] = 0;
    }
    OmMarketSubscription subs[2] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 2, .product_id = 0}
    };
    OmMarketConfig cfg = {
        .max_products = 4,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = org_to_worker,
        .subs = subs,
        .sub_count = 2,
        .expected_orders_per_worker = 4,
        .expected_subscribers_per_product = 1,
        .expected_price_levels = 4,
        .top_levels = 1,
        .dealable = test_marketable,
        .dealable_ctx = NULL
    };

    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketWorker *worker = om_market_worker(&market, 0);
    ck_assert_ptr_nonnull(worker);

    OmWalInsert insert = {
        .order_id = 200,
        .price = 20,
        .volume = 40,
        .vol_remain = 40,
        .org = 1,
        .flags = OM_SIDE_BID,
        .product_id = 0
    };
    ck_assert_int_eq(om_market_worker_process(worker, OM_WAL_INSERT, &insert), 0);
    ck_assert_int_eq(om_market_public_process(&market.public_workers[0], OM_WAL_INSERT, &insert), 0);

    OmWalInsert insert_unsub = {
        .order_id = 201,
        .price = 21,
        .volume = 10,
        .vol_remain = 10,
        .org = 1,
        .flags = OM_SIDE_BID,
        .product_id = 1
    };
    ck_assert_int_eq(om_market_worker_process(worker, OM_WAL_INSERT, &insert_unsub), 0);
    ck_assert_int_eq(om_market_public_process(&market.public_workers[0], OM_WAL_INSERT,
                                               &insert_unsub), 0);

    OmMarketDelta deltas[4];
    ck_assert_int_eq(om_market_worker_delta_count(worker, 2, 0, OM_SIDE_BID), 1);
    ck_assert_int_eq(om_market_worker_copy_deltas(worker, 2, 0, OM_SIDE_BID, deltas, 4), 1);
    ck_assert_uint_eq(deltas[0].price, 20);
    ck_assert_int_eq(deltas[0].delta, 40);
    ck_assert_int_eq(om_market_worker_clear_deltas(worker, 2, 0, OM_SIDE_BID), 0);
    ck_assert_int_eq(om_market_worker_delta_count(worker, 2, 0, OM_SIDE_BID), 0);

    ck_assert_int_eq(om_market_worker_copy_full(worker, 2, 0, OM_SIDE_BID, deltas, 4), 1);
    ck_assert_uint_eq(deltas[0].price, 20);
    ck_assert_int_eq(deltas[0].delta, 40);
    ck_assert_int_eq(om_market_worker_clear_dirty(worker, 2, 0), 0);

    ck_assert_int_eq(om_market_worker_delta_count(worker, 2, 1, OM_SIDE_BID), OM_ERR_NOT_SUBSCRIBED);
    ck_assert_int_eq(om_market_worker_copy_deltas(worker, 2, 1, OM_SIDE_BID, deltas, 4), OM_ERR_NOT_SUBSCRIBED);
    ck_assert_int_eq(om_market_worker_copy_full(worker, 2, 1, OM_SIDE_BID, deltas, 4), OM_ERR_NOT_SUBSCRIBED);

    ck_assert_int_eq(om_market_public_delta_count(&market.public_workers[0], 0, OM_SIDE_BID), 1);
    ck_assert_int_eq(om_market_public_copy_deltas(&market.public_workers[0], 0, OM_SIDE_BID,
                                                  deltas, 4), 1);
    ck_assert_uint_eq(deltas[0].price, 20);
    ck_assert_int_eq(deltas[0].delta, 40);
    ck_assert_int_eq(om_market_public_clear_deltas(&market.public_workers[0], 0, OM_SIDE_BID), 0);
    ck_assert_int_eq(om_market_public_delta_count(&market.public_workers[0], 0, OM_SIDE_BID), 0);

    ck_assert_int_eq(om_market_public_copy_full(&market.public_workers[0], 0, OM_SIDE_BID,
                                                deltas, 4), 1);
    ck_assert_uint_eq(deltas[0].price, 20);
    ck_assert_int_eq(deltas[0].delta, 40);
    ck_assert_int_eq(om_market_public_clear_dirty(&market.public_workers[0], 0), 0);

    ck_assert_int_eq(om_market_public_delta_count(&market.public_workers[0], 1, OM_SIDE_BID), OM_ERR_NOT_SUBSCRIBED);
    ck_assert_int_eq(om_market_public_copy_deltas(&market.public_workers[0], 1, OM_SIDE_BID,
                                                  deltas, 4), OM_ERR_NOT_SUBSCRIBED);
    ck_assert_int_eq(om_market_public_copy_full(&market.public_workers[0], 1, OM_SIDE_BID,
                                                deltas, 4), OM_ERR_NOT_SUBSCRIBED);

    om_market_destroy(&market);
}
END_TEST

/* Test that CANCEL affects ALL orgs that can see the order */
START_TEST(test_market_multi_org_visibility) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) {
        org_to_worker[i] = 0;
    }
    /* 3 orgs subscribed to product 0 */
    OmMarketSubscription subs[3] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 2, .product_id = 0},
        {.org_id = 3, .product_id = 0}
    };
    OmMarketConfig cfg = {
        .max_products = 4,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = org_to_worker,
        .subs = subs,
        .sub_count = 3,
        .expected_orders_per_worker = 4,
        .expected_subscribers_per_product = 3,
        .expected_price_levels = 4,
        .top_levels = 2,
        .dealable = test_multi_org_marketable,
        .dealable_ctx = NULL
    };

    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketWorker *worker = om_market_worker(&market, 0);
    ck_assert_ptr_nonnull(worker);

    /* Insert order from org 1 - should be visible to both org 2 and org 3 */
    OmWalInsert insert = {
        .order_id = 300,
        .price = 100,
        .volume = 50,
        .vol_remain = 50,
        .org = 1,
        .flags = OM_SIDE_BID,
        .product_id = 0
    };
    ck_assert_int_eq(om_market_worker_process(worker, OM_WAL_INSERT, &insert), 0);

    /* Verify org 2 sees the order */
    uint64_t qty = 0;
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 50);

    /* Verify org 3 also sees the order */
    ck_assert_int_eq(om_market_worker_get_qty(worker, 3, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 50);

    /* Org 1 should NOT see its own order (dealable returns 0) */
    ck_assert_int_ne(om_market_worker_get_qty(worker, 1, 0, OM_SIDE_BID, 100, &qty), 0);

    /* Clear dirty flags before testing CANCEL */
    om_market_worker_clear_dirty(worker, 2, 0);
    om_market_worker_clear_dirty(worker, 3, 0);

    /* CANCEL - should remove qty from BOTH org 2 and org 3 */
    OmWalCancel cancel = {
        .order_id = 300,
        .product_id = 0
    };
    ck_assert_int_eq(om_market_worker_process(worker, OM_WAL_CANCEL, &cancel), 0);

    /* Both orgs should now see 0 qty at this price */
    ck_assert_int_ne(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_int_ne(om_market_worker_get_qty(worker, 3, 0, OM_SIDE_BID, 100, &qty), 0);

    /* Both ladders should be marked dirty */
    ck_assert_int_eq(om_market_worker_is_dirty(worker, 2, 0), 1);
    ck_assert_int_eq(om_market_worker_is_dirty(worker, 3, 0), 1);

    om_market_destroy(&market);
}
END_TEST

/* Test dynamic ladder: all prices are tracked, top-N applied at publish time */
START_TEST(test_market_dynamic_ladder) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) {
        org_to_worker[i] = 0;
    }
    OmMarketSubscription subs[2] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 2, .product_id = 0}
    };
    OmMarketConfig cfg = {
        .max_products = 4,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = org_to_worker,
        .subs = subs,
        .sub_count = 2,
        .expected_orders_per_worker = 8,
        .expected_subscribers_per_product = 2,
        .expected_price_levels = 8,
        .top_levels = 3,  /* Publish top 3 price levels */
        .dealable = test_marketable,
        .dealable_ctx = NULL
    };

    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketWorker *worker = om_market_worker(&market, 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];
    ck_assert_ptr_nonnull(worker);

    /* Insert 4 bid orders at prices 100, 90, 80, 70 (from org 1, visible to org 2) */
    OmWalInsert orders[4] = {
        {.order_id = 1, .price = 100, .volume = 10, .vol_remain = 10, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 2, .price = 90, .volume = 20, .vol_remain = 20, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 3, .price = 80, .volume = 30, .vol_remain = 30, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 4, .price = 70, .volume = 40, .vol_remain = 40, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0}
    };

    for (int i = 0; i < 4; i++) {
        ck_assert_int_eq(om_market_worker_process(worker, OM_WAL_INSERT, &orders[i]), 0);
        ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &orders[i]), 0);
    }

    /* ALL prices are tracked in the ladder (dynamic, not capped) */
    uint64_t qty = 0;
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 10);
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 90, &qty), 0);
    ck_assert_uint_eq(qty, 20);
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 80, &qty), 0);
    ck_assert_uint_eq(qty, 30);
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 70, &qty), 0);
    ck_assert_uint_eq(qty, 40);  /* Price 70 is also tracked! */

    /* Public worker also tracks all prices */
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 90, &qty), 0);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 80, &qty), 0);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 70, &qty), 0);

    /* But copy_full with max=3 returns only top-3 */
    OmMarketDelta full[4];
    int n = om_market_public_copy_full(pub, 0, OM_SIDE_BID, full, 3);
    ck_assert_int_eq(n, 3);
    /* Sorted descending (best bids first): 100, 90, 80 */
    ck_assert_uint_eq(full[0].price, 100);
    ck_assert_int_eq(full[0].delta, 10);
    ck_assert_uint_eq(full[1].price, 90);
    ck_assert_int_eq(full[1].delta, 20);
    ck_assert_uint_eq(full[2].price, 80);
    ck_assert_int_eq(full[2].delta, 30);

    /* Cancel order at price 90 */
    OmWalCancel cancel = {.order_id = 2, .product_id = 0};
    ck_assert_int_eq(om_market_worker_process(worker, OM_WAL_CANCEL, &cancel), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_CANCEL, &cancel), 0);

    /* Price 90 is removed, remaining: 100, 80, 70 */
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 10);
    ck_assert_int_ne(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 90, &qty), 0);  /* Removed */
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 80, &qty), 0);
    ck_assert_uint_eq(qty, 30);
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 70, &qty), 0);
    ck_assert_uint_eq(qty, 40);

    /* Now copy_full with max=3 returns: 100, 80, 70 (70 naturally appears as top-3) */
    n = om_market_public_copy_full(pub, 0, OM_SIDE_BID, full, 3);
    ck_assert_int_eq(n, 3);
    ck_assert_uint_eq(full[0].price, 100);
    ck_assert_int_eq(full[0].delta, 10);
    ck_assert_uint_eq(full[1].price, 80);
    ck_assert_int_eq(full[1].delta, 30);
    ck_assert_uint_eq(full[2].price, 70);
    ck_assert_int_eq(full[2].delta, 40);

    om_market_destroy(&market);
}
END_TEST

/* Test dynamic ladder for ask side (ascending order) */
START_TEST(test_market_dynamic_ladder_ask) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) {
        org_to_worker[i] = 0;
    }
    OmMarketSubscription subs[2] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 2, .product_id = 0}
    };
    OmMarketConfig cfg = {
        .max_products = 4,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = org_to_worker,
        .subs = subs,
        .sub_count = 2,
        .expected_orders_per_worker = 8,
        .expected_subscribers_per_product = 2,
        .expected_price_levels = 8,
        .top_levels = 3,  /* Publish top 3 price levels */
        .dealable = test_marketable,
        .dealable_ctx = NULL
    };

    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];
    ck_assert_ptr_nonnull(pub->slab.slots);
    ck_assert_uint_eq((uintptr_t)pub->slab.slots % 64U, 0U);

    /* Insert 4 ask orders at prices 10, 20, 30, 40 */
    OmWalInsert orders[4] = {
        {.order_id = 1, .price = 10, .volume = 10, .vol_remain = 10, .org = 1,
         .flags = OM_SIDE_ASK, .product_id = 0},
        {.order_id = 2, .price = 20, .volume = 20, .vol_remain = 20, .org = 1,
         .flags = OM_SIDE_ASK, .product_id = 0},
        {.order_id = 3, .price = 30, .volume = 30, .vol_remain = 30, .org = 1,
         .flags = OM_SIDE_ASK, .product_id = 0},
        {.order_id = 4, .price = 40, .volume = 40, .vol_remain = 40, .org = 1,
         .flags = OM_SIDE_ASK, .product_id = 0}
    };

    for (int i = 0; i < 4; i++) {
        ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &orders[i]), 0);
    }

    /* All 4 prices are tracked */
    uint64_t qty = 0;
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_ASK, 10, &qty), 0);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_ASK, 20, &qty), 0);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_ASK, 30, &qty), 0);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_ASK, 40, &qty), 0);

    /* Copy_full with max=3 returns top-3 asks (ascending: 10, 20, 30) */
    OmMarketDelta full[4];
    int n = om_market_public_copy_full(pub, 0, OM_SIDE_ASK, full, 3);
    ck_assert_int_eq(n, 3);
    ck_assert_uint_eq(full[0].price, 10);
    ck_assert_uint_eq(full[1].price, 20);
    ck_assert_uint_eq(full[2].price, 30);

    /* Cancel order at price 20 */
    OmWalCancel cancel = {.order_id = 2, .product_id = 0};
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_CANCEL, &cancel), 0);

    /* Now top-3 should be: 10, 30, 40 */
    n = om_market_public_copy_full(pub, 0, OM_SIDE_ASK, full, 3);
    ck_assert_int_eq(n, 3);
    ck_assert_uint_eq(full[0].price, 10);
    ck_assert_uint_eq(full[1].price, 30);
    ck_assert_uint_eq(full[2].price, 40);

    om_market_destroy(&market);
}
END_TEST

/* Test dynamic ladder with MATCH removing a level */
START_TEST(test_market_dynamic_ladder_match) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) {
        org_to_worker[i] = 0;
    }
    OmMarketSubscription subs[2] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 2, .product_id = 0}
    };
    OmMarketConfig cfg = {
        .max_products = 4,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = org_to_worker,
        .subs = subs,
        .sub_count = 2,
        .expected_orders_per_worker = 8,
        .expected_subscribers_per_product = 2,
        .expected_price_levels = 8,
        .top_levels = 2,  /* Publish top 2 price levels */
        .dealable = test_marketable,
        .dealable_ctx = NULL
    };

    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];

    /* Insert 3 bid orders */
    OmWalInsert orders[3] = {
        {.order_id = 1, .price = 100, .volume = 10, .vol_remain = 10, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 2, .price = 90, .volume = 20, .vol_remain = 20, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 3, .price = 80, .volume = 30, .vol_remain = 30, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0}
    };

    for (int i = 0; i < 3; i++) {
        ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &orders[i]), 0);
    }

    /* All 3 prices are tracked */
    uint64_t qty = 0;
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 90, &qty), 0);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 80, &qty), 0);

    /* Match fully consumes order at price 100 */
    OmWalMatch match = {.maker_id = 1, .taker_id = 999, .volume = 10, .price = 100};
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_MATCH, &match), 0);

    /* Price 100 is removed */
    ck_assert_int_ne(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 100, &qty), 0);  /* Removed */
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 90, &qty), 0);
    ck_assert_uint_eq(qty, 20);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 80, &qty), 0);
    ck_assert_uint_eq(qty, 30);

    /* Copy_full with max=2 returns top-2: 90, 80 */
    OmMarketDelta full[3];
    int n = om_market_public_copy_full(pub, 0, OM_SIDE_BID, full, 2);
    ck_assert_int_eq(n, 2);
    ck_assert_uint_eq(full[0].price, 90);
    ck_assert_uint_eq(full[1].price, 80);

    om_market_destroy(&market);
}
END_TEST

/* Public: multiple orders at same price accumulate qty */
START_TEST(test_public_same_price_accumulates) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) org_to_worker[i] = 0;
    OmMarketSubscription subs[1] = {{.org_id = 1, .product_id = 0}};
    OmMarketConfig cfg = {
        .max_products = 4, .worker_count = 1, .public_worker_count = 1,
        .org_to_worker = org_to_worker, .product_to_public_worker = org_to_worker,
        .subs = subs, .sub_count = 1,
        .expected_orders_per_worker = 8, .expected_subscribers_per_product = 1,
        .expected_price_levels = 8, .top_levels = 5,
        .dealable = test_marketable, .dealable_ctx = NULL
    };
    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];

    /* 3 orders at price 100 from different orgs */
    OmWalInsert ins[] = {
        {.order_id = 1, .price = 100, .volume = 10, .vol_remain = 10, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 2, .price = 100, .volume = 20, .vol_remain = 20, .org = 2,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 3, .price = 100, .volume = 30, .vol_remain = 30, .org = 3,
         .flags = OM_SIDE_BID, .product_id = 0},
    };
    for (int i = 0; i < 3; i++)
        ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &ins[i]), 0);

    /* Public sees accumulated qty = 10 + 20 + 30 = 60 */
    uint64_t qty = 0;
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 60);

    /* Cancel one order -> qty decremented but level remains */
    OmWalCancel cancel = {.order_id = 2, .product_id = 0};
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_CANCEL, &cancel), 0);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 40);  /* 60 - 20 */

    /* Cancel remaining orders -> level removed */
    OmWalCancel c1 = {.order_id = 1, .product_id = 0};
    OmWalCancel c3 = {.order_id = 3, .product_id = 0};
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_CANCEL, &c1), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_CANCEL, &c3), 0);
    ck_assert_int_ne(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 100, &qty), 0);

    om_market_destroy(&market);
}
END_TEST

/* Public: partial match decrements qty, level stays */
START_TEST(test_public_partial_match) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) org_to_worker[i] = 0;
    OmMarketSubscription subs[1] = {{.org_id = 1, .product_id = 0}};
    OmMarketConfig cfg = {
        .max_products = 4, .worker_count = 1, .public_worker_count = 1,
        .org_to_worker = org_to_worker, .product_to_public_worker = org_to_worker,
        .subs = subs, .sub_count = 1,
        .expected_orders_per_worker = 8, .expected_subscribers_per_product = 1,
        .expected_price_levels = 8, .top_levels = 5,
        .dealable = test_marketable, .dealable_ctx = NULL
    };
    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];

    OmWalInsert ins = {.order_id = 1, .price = 50, .volume = 100, .vol_remain = 100,
                       .org = 1, .flags = OM_SIDE_ASK, .product_id = 0};
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &ins), 0);

    /* Partial match: 30 out of 100 */
    OmWalMatch match = {.maker_id = 1, .taker_id = 99, .volume = 30, .price = 50};
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_MATCH, &match), 0);

    uint64_t qty = 0;
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_ASK, 50, &qty), 0);
    ck_assert_uint_eq(qty, 70);  /* 100 - 30 */

    /* Another partial match: 40 more */
    OmWalMatch match2 = {.maker_id = 1, .taker_id = 98, .volume = 40, .price = 50};
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_MATCH, &match2), 0);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_ASK, 50, &qty), 0);
    ck_assert_uint_eq(qty, 30);  /* 70 - 40 */

    /* Full match: remaining 30 */
    OmWalMatch match3 = {.maker_id = 1, .taker_id = 97, .volume = 30, .price = 50};
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_MATCH, &match3), 0);
    ck_assert_int_ne(om_market_public_get_qty(pub, 0, OM_SIDE_ASK, 50, &qty), 0);  /* Gone */

    om_market_destroy(&market);
}
END_TEST

/* Public: bid and ask queues are independent in the same ladder */
START_TEST(test_public_bid_ask_independent) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) org_to_worker[i] = 0;
    OmMarketSubscription subs[1] = {{.org_id = 1, .product_id = 0}};
    OmMarketConfig cfg = {
        .max_products = 4, .worker_count = 1, .public_worker_count = 1,
        .org_to_worker = org_to_worker, .product_to_public_worker = org_to_worker,
        .subs = subs, .sub_count = 1,
        .expected_orders_per_worker = 8, .expected_subscribers_per_product = 1,
        .expected_price_levels = 8, .top_levels = 5,
        .dealable = test_marketable, .dealable_ctx = NULL
    };
    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];

    /* Insert bids and asks at overlapping prices */
    OmWalInsert bids[] = {
        {.order_id = 1, .price = 100, .volume = 10, .vol_remain = 10, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 2, .price = 99, .volume = 20, .vol_remain = 20, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
    };
    OmWalInsert asks[] = {
        {.order_id = 3, .price = 101, .volume = 30, .vol_remain = 30, .org = 1,
         .flags = OM_SIDE_ASK, .product_id = 0},
        {.order_id = 4, .price = 102, .volume = 40, .vol_remain = 40, .org = 1,
         .flags = OM_SIDE_ASK, .product_id = 0},
    };
    for (int i = 0; i < 2; i++) {
        ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &bids[i]), 0);
        ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &asks[i]), 0);
    }

    /* Verify bid Q1 is descending */
    OmMarketDelta full[4];
    int n = om_market_public_copy_full(pub, 0, OM_SIDE_BID, full, 4);
    ck_assert_int_eq(n, 2);
    ck_assert_uint_eq(full[0].price, 100);
    ck_assert_uint_eq(full[1].price, 99);

    /* Verify ask Q1 is ascending */
    n = om_market_public_copy_full(pub, 0, OM_SIDE_ASK, full, 4);
    ck_assert_int_eq(n, 2);
    ck_assert_uint_eq(full[0].price, 101);
    ck_assert_uint_eq(full[1].price, 102);

    /* Cancel best bid - ask unaffected */
    OmWalCancel cancel = {.order_id = 1, .product_id = 0};
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_CANCEL, &cancel), 0);

    n = om_market_public_copy_full(pub, 0, OM_SIDE_BID, full, 4);
    ck_assert_int_eq(n, 1);
    ck_assert_uint_eq(full[0].price, 99);

    n = om_market_public_copy_full(pub, 0, OM_SIDE_ASK, full, 4);
    ck_assert_int_eq(n, 2);  /* Still 2 ask levels */

    om_market_destroy(&market);
}
END_TEST

/* Public: slot recycling - cancel frees slot, new insert reuses it */
START_TEST(test_public_slot_recycling) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) org_to_worker[i] = 0;
    OmMarketSubscription subs[1] = {{.org_id = 1, .product_id = 0}};
    OmMarketConfig cfg = {
        .max_products = 4, .worker_count = 1, .public_worker_count = 1,
        .org_to_worker = org_to_worker, .product_to_public_worker = org_to_worker,
        .subs = subs, .sub_count = 1,
        .expected_orders_per_worker = 8, .expected_subscribers_per_product = 1,
        .expected_price_levels = 4, .top_levels = 10,
        .dealable = test_marketable, .dealable_ctx = NULL
    };
    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];

    /* Insert and cancel many times - slots should be recycled */
    for (uint64_t round = 0; round < 50; round++) {
        OmWalInsert ins = {.order_id = 1000 + round, .price = 200 + round,
                           .volume = 5, .vol_remain = 5, .org = 1,
                           .flags = OM_SIDE_BID, .product_id = 0};
        ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &ins), 0);

        uint64_t qty = 0;
        ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 200 + round, &qty), 0);
        ck_assert_uint_eq(qty, 5);

        OmWalCancel cancel = {.order_id = 1000 + round, .product_id = 0};
        ck_assert_int_eq(om_market_public_process(pub, OM_WAL_CANCEL, &cancel), 0);
        ck_assert_int_ne(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 200 + round, &qty), 0);
    }

    /* After all cycles, ladder should be empty */
    OmMarketDelta full[4];
    int n = om_market_public_copy_full(pub, 0, OM_SIDE_BID, full, 4);
    ck_assert_int_eq(n, 0);

    om_market_destroy(&market);
}
END_TEST

/* Public: multiple products in one worker, independent ladders */
START_TEST(test_public_multi_product) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) org_to_worker[i] = 0;
    OmMarketSubscription subs[] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 1, .product_id = 1},
        {.org_id = 1, .product_id = 2},
    };
    OmMarketConfig cfg = {
        .max_products = 4, .worker_count = 1, .public_worker_count = 1,
        .org_to_worker = org_to_worker, .product_to_public_worker = org_to_worker,
        .subs = subs, .sub_count = 3,
        .expected_orders_per_worker = 8, .expected_subscribers_per_product = 1,
        .expected_price_levels = 8, .top_levels = 5,
        .dealable = test_marketable, .dealable_ctx = NULL
    };
    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];

    /* Insert into 3 products */
    OmWalInsert ins[] = {
        {.order_id = 1, .price = 100, .volume = 10, .vol_remain = 10, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 2, .price = 200, .volume = 20, .vol_remain = 20, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 1},
        {.order_id = 3, .price = 300, .volume = 30, .vol_remain = 30, .org = 1,
         .flags = OM_SIDE_ASK, .product_id = 2},
    };
    for (int i = 0; i < 3; i++)
        ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &ins[i]), 0);

    /* Each product has independent ladder */
    uint64_t qty = 0;
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 10);
    ck_assert_int_ne(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 200, &qty), 0);  /* Wrong product */

    ck_assert_int_eq(om_market_public_get_qty(pub, 1, OM_SIDE_BID, 200, &qty), 0);
    ck_assert_uint_eq(qty, 20);

    ck_assert_int_eq(om_market_public_get_qty(pub, 2, OM_SIDE_ASK, 300, &qty), 0);
    ck_assert_uint_eq(qty, 30);

    /* Cancel in product 1 doesn't affect product 0 or 2 */
    OmWalCancel cancel = {.order_id = 2, .product_id = 1};
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_CANCEL, &cancel), 0);

    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 10);  /* Unaffected */
    ck_assert_int_ne(om_market_public_get_qty(pub, 1, OM_SIDE_BID, 200, &qty), 0);  /* Gone */
    ck_assert_int_eq(om_market_public_get_qty(pub, 2, OM_SIDE_ASK, 300, &qty), 0);
    ck_assert_uint_eq(qty, 30);  /* Unaffected */

    om_market_destroy(&market);
}
END_TEST

/* Custom dealable: org 2 sees full qty, org 3 sees half */
static uint64_t test_half_dealable(const OmWalInsert *rec, uint16_t viewer_org, void *ctx) {
    (void)ctx;
    if (rec->org == viewer_org) return 0;
    if (viewer_org == 3) return rec->vol_remain / 2;
    return rec->vol_remain;
}

/* Private: fan-out - same order seen with different qty per org via dealable */
START_TEST(test_private_fanout_different_qty) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) org_to_worker[i] = 0;
    OmMarketSubscription subs[] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 2, .product_id = 0},
        {.org_id = 3, .product_id = 0},
    };
    OmMarketConfig cfg = {
        .max_products = 4, .worker_count = 1, .public_worker_count = 1,
        .org_to_worker = org_to_worker, .product_to_public_worker = org_to_worker,
        .subs = subs, .sub_count = 3,
        .expected_orders_per_worker = 8, .expected_subscribers_per_product = 3,
        .expected_price_levels = 8, .top_levels = 5,
        .dealable = test_half_dealable, .dealable_ctx = NULL
    };
    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketWorker *w = om_market_worker(&market, 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];

    OmWalInsert ins = {.order_id = 1, .price = 50, .volume = 100, .vol_remain = 100,
                       .org = 1, .flags = OM_SIDE_BID, .product_id = 0};
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_INSERT, &ins), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &ins), 0);

    uint64_t qty = 0;
    /* Org 1 can't see own order */
    ck_assert_int_ne(om_market_worker_get_qty(w, 1, 0, OM_SIDE_BID, 50, &qty), 0);
    /* Org 2 sees full qty */
    ck_assert_int_eq(om_market_worker_get_qty(w, 2, 0, OM_SIDE_BID, 50, &qty), 0);
    ck_assert_uint_eq(qty, 100);
    /* Org 3 sees half */
    ck_assert_int_eq(om_market_worker_get_qty(w, 3, 0, OM_SIDE_BID, 50, &qty), 0);
    ck_assert_uint_eq(qty, 50);
    /* Public sees total (raw vol_remain) */
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 50, &qty), 0);
    ck_assert_uint_eq(qty, 100);

    om_market_destroy(&market);
}
END_TEST

/* Private: match decrements all viewing orgs */
START_TEST(test_private_match_fanout) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) org_to_worker[i] = 0;
    OmMarketSubscription subs[] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 2, .product_id = 0},
        {.org_id = 3, .product_id = 0},
    };
    OmMarketConfig cfg = {
        .max_products = 4, .worker_count = 1, .public_worker_count = 1,
        .org_to_worker = org_to_worker, .product_to_public_worker = org_to_worker,
        .subs = subs, .sub_count = 3,
        .expected_orders_per_worker = 8, .expected_subscribers_per_product = 3,
        .expected_price_levels = 8, .top_levels = 5,
        .dealable = test_multi_org_marketable, .dealable_ctx = NULL
    };
    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketWorker *w = om_market_worker(&market, 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];

    /* Org 1 inserts order visible to org 2 and org 3 */
    OmWalInsert ins = {.order_id = 1, .price = 80, .volume = 100, .vol_remain = 100,
                       .org = 1, .flags = OM_SIDE_BID, .product_id = 0};
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_INSERT, &ins), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &ins), 0);

    /* Partial match: 40 units */
    OmWalMatch match = {.maker_id = 1, .taker_id = 99, .volume = 40, .price = 80};
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_MATCH, &match), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_MATCH, &match), 0);

    uint64_t qty = 0;
    /* Both viewing orgs decremented */
    ck_assert_int_eq(om_market_worker_get_qty(w, 2, 0, OM_SIDE_BID, 80, &qty), 0);
    ck_assert_uint_eq(qty, 60);
    ck_assert_int_eq(om_market_worker_get_qty(w, 3, 0, OM_SIDE_BID, 80, &qty), 0);
    ck_assert_uint_eq(qty, 60);
    /* Public also decremented */
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 80, &qty), 0);
    ck_assert_uint_eq(qty, 60);

    /* Both private ladders dirty */
    ck_assert_int_eq(om_market_worker_is_dirty(w, 2, 0), 1);
    ck_assert_int_eq(om_market_worker_is_dirty(w, 3, 0), 1);
    ck_assert_int_eq(om_market_public_is_dirty(pub, 0), 1);

    om_market_destroy(&market);
}
END_TEST

/* Private: copy_full top-N ordering */
START_TEST(test_private_copy_full_topn) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) org_to_worker[i] = 0;
    OmMarketSubscription subs[] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 2, .product_id = 0},
    };
    OmMarketConfig cfg = {
        .max_products = 4, .worker_count = 1, .public_worker_count = 1,
        .org_to_worker = org_to_worker, .product_to_public_worker = org_to_worker,
        .subs = subs, .sub_count = 2,
        .expected_orders_per_worker = 8, .expected_subscribers_per_product = 2,
        .expected_price_levels = 8, .top_levels = 3,
        .dealable = test_marketable, .dealable_ctx = NULL
    };
    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketWorker *w = om_market_worker(&market, 0);

    /* Insert 5 bid orders from org 1 - visible to org 2 */
    OmWalInsert ins[] = {
        {.order_id = 1, .price = 100, .volume = 10, .vol_remain = 10, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 2, .price = 95, .volume = 20, .vol_remain = 20, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 3, .price = 90, .volume = 30, .vol_remain = 30, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 4, .price = 85, .volume = 40, .vol_remain = 40, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 5, .price = 80, .volume = 50, .vol_remain = 50, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
    };
    for (int i = 0; i < 5; i++)
        ck_assert_int_eq(om_market_worker_process(w, OM_WAL_INSERT, &ins[i]), 0);

    /* copy_full with max=3 on private worker -> top-3 bids descending */
    OmMarketDelta full[5];
    int n = om_market_worker_copy_full(w, 2, 0, OM_SIDE_BID, full, 3);
    ck_assert_int_eq(n, 3);
    ck_assert_uint_eq(full[0].price, 100);
    ck_assert_int_eq(full[0].delta, 10);
    ck_assert_uint_eq(full[1].price, 95);
    ck_assert_int_eq(full[1].delta, 20);
    ck_assert_uint_eq(full[2].price, 90);
    ck_assert_int_eq(full[2].delta, 30);

    /* copy_full with max=5 returns all 5 in order */
    n = om_market_worker_copy_full(w, 2, 0, OM_SIDE_BID, full, 5);
    ck_assert_int_eq(n, 5);
    ck_assert_uint_eq(full[0].price, 100);
    ck_assert_uint_eq(full[4].price, 80);

    om_market_destroy(&market);
}
END_TEST

/* Private: slab growth under fan-out pressure (N orgs × M prices) */
START_TEST(test_private_slab_growth_fanout) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) org_to_worker[i] = 0;

    /* 5 orgs subscribed to same product -> 5x fan-out */
    OmMarketSubscription subs[5];
    for (int i = 0; i < 5; i++) {
        subs[i].org_id = (uint16_t)(i + 1);
        subs[i].product_id = 0;
    }
    OmMarketConfig cfg = {
        .max_products = 4, .worker_count = 1, .public_worker_count = 1,
        .org_to_worker = org_to_worker, .product_to_public_worker = org_to_worker,
        .subs = subs, .sub_count = 5,
        .expected_orders_per_worker = 8, .expected_subscribers_per_product = 5,
        .expected_price_levels = 2,  /* Tiny slab to force growth */
        .top_levels = 50,
        .dealable = test_marketable, .dealable_ctx = NULL
    };
    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketWorker *w = om_market_worker(&market, 0);

    /* Insert 30 orders from org 1 -> each fans out to 4 orgs (2,3,4,5)
     * That's 30 × 4 = 120 private slots needed */
    for (uint64_t i = 0; i < 30; i++) {
        OmWalInsert ins = {.order_id = i + 1, .price = 500 + i, .volume = 10,
                           .vol_remain = 10, .org = 1,
                           .flags = OM_SIDE_BID, .product_id = 0};
        ck_assert_int_eq(om_market_worker_process(w, OM_WAL_INSERT, &ins), 0);
    }

    /* All 4 viewing orgs should see all 30 price levels */
    for (uint16_t org = 2; org <= 5; org++) {
        uint64_t qty = 0;
        for (uint64_t i = 0; i < 30; i++) {
            ck_assert_int_eq(om_market_worker_get_qty(w, org, 0, OM_SIDE_BID, 500 + i, &qty), 0);
            ck_assert_uint_eq(qty, 10);
        }
        /* Verify sorted order via copy_full */
        OmMarketDelta full[30];
        int n = om_market_worker_copy_full(w, org, 0, OM_SIDE_BID, full, 30);
        ck_assert_int_eq(n, 30);
        ck_assert_uint_eq(full[0].price, 529);   /* Best bid */
        ck_assert_uint_eq(full[29].price, 500);  /* Worst bid */
    }

    om_market_destroy(&market);
}
END_TEST

/* Private+Public: different views simultaneously */
START_TEST(test_private_public_different_views) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) org_to_worker[i] = 0;
    OmMarketSubscription subs[] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 2, .product_id = 0},
    };
    OmMarketConfig cfg = {
        .max_products = 4, .worker_count = 1, .public_worker_count = 1,
        .org_to_worker = org_to_worker, .product_to_public_worker = org_to_worker,
        .subs = subs, .sub_count = 2,
        .expected_orders_per_worker = 8, .expected_subscribers_per_product = 2,
        .expected_price_levels = 8, .top_levels = 5,
        .dealable = test_marketable, .dealable_ctx = NULL
    };
    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketWorker *w = om_market_worker(&market, 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];

    /* Org 1 and org 2 each insert a bid at same price */
    OmWalInsert ins1 = {.order_id = 1, .price = 100, .volume = 60, .vol_remain = 60,
                        .org = 1, .flags = OM_SIDE_BID, .product_id = 0};
    OmWalInsert ins2 = {.order_id = 2, .price = 100, .volume = 40, .vol_remain = 40,
                        .org = 2, .flags = OM_SIDE_BID, .product_id = 0};
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_INSERT, &ins1), 0);
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_INSERT, &ins2), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &ins1), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &ins2), 0);

    uint64_t qty = 0;
    /* Public sees total: 60 + 40 = 100 */
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 100);

    /* Org 1 private: can't see own (60), sees org 2's (40) */
    ck_assert_int_eq(om_market_worker_get_qty(w, 1, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 40);

    /* Org 2 private: can't see own (40), sees org 1's (60) */
    ck_assert_int_eq(om_market_worker_get_qty(w, 2, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 60);

    /* Cancel org 1's order */
    OmWalCancel cancel = {.order_id = 1, .product_id = 0};
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_CANCEL, &cancel), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_CANCEL, &cancel), 0);

    /* Public: 100 - 60 = 40 */
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 40);

    /* Org 2 private: org 1's order gone, level removed for org 2 */
    ck_assert_int_ne(om_market_worker_get_qty(w, 2, 0, OM_SIDE_BID, 100, &qty), 0);

    /* Org 1 private: still sees org 2's order */
    ck_assert_int_eq(om_market_worker_get_qty(w, 1, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 40);

    om_market_destroy(&market);
}
END_TEST

/* Slab: Q1 sorted order preserved across insert-cancel-insert cycles */
START_TEST(test_slab_q1_order_after_churn) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) org_to_worker[i] = 0;
    OmMarketSubscription subs[1] = {{.org_id = 1, .product_id = 0}};
    OmMarketConfig cfg = {
        .max_products = 4, .worker_count = 1, .public_worker_count = 1,
        .org_to_worker = org_to_worker, .product_to_public_worker = org_to_worker,
        .subs = subs, .sub_count = 1,
        .expected_orders_per_worker = 8, .expected_subscribers_per_product = 1,
        .expected_price_levels = 8, .top_levels = 10,
        .dealable = test_marketable, .dealable_ctx = NULL
    };
    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];

    /* Build initial ladder: 100, 90, 80, 70, 60 */
    for (int i = 0; i < 5; i++) {
        OmWalInsert ins = {.order_id = (uint64_t)(i + 1), .price = (uint64_t)(100 - i * 10),
                           .volume = 10, .vol_remain = 10, .org = 1,
                           .flags = OM_SIDE_BID, .product_id = 0};
        ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &ins), 0);
    }

    /* Cancel middle prices: 90, 70 */
    OmWalCancel c1 = {.order_id = 2, .product_id = 0};  /* price 90 */
    OmWalCancel c2 = {.order_id = 4, .product_id = 0};  /* price 70 */
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_CANCEL, &c1), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_CANCEL, &c2), 0);

    /* Remaining: 100, 80, 60 */
    OmMarketDelta full[5];
    int n = om_market_public_copy_full(pub, 0, OM_SIDE_BID, full, 5);
    ck_assert_int_eq(n, 3);
    ck_assert_uint_eq(full[0].price, 100);
    ck_assert_uint_eq(full[1].price, 80);
    ck_assert_uint_eq(full[2].price, 60);

    /* Insert new prices in gaps: 95, 75, 55 */
    OmWalInsert gap[] = {
        {.order_id = 10, .price = 95, .volume = 15, .vol_remain = 15, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 11, .price = 75, .volume = 25, .vol_remain = 25, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 12, .price = 55, .volume = 35, .vol_remain = 35, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
    };
    for (int i = 0; i < 3; i++)
        ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &gap[i]), 0);

    /* Verify sorted order: 100, 95, 80, 75, 60, 55 */
    OmMarketDelta full6[6];
    n = om_market_public_copy_full(pub, 0, OM_SIDE_BID, full6, 6);
    ck_assert_int_eq(n, 6);
    ck_assert_uint_eq(full6[0].price, 100);
    ck_assert_uint_eq(full6[1].price, 95);
    ck_assert_uint_eq(full6[2].price, 80);
    ck_assert_uint_eq(full6[3].price, 75);
    ck_assert_uint_eq(full6[4].price, 60);
    ck_assert_uint_eq(full6[5].price, 55);

    om_market_destroy(&market);
}
END_TEST

/* DEACTIVATE currently follows CANCEL semantics; ACTIVATE is a no-op after remaining==0 */
START_TEST(test_market_deactivate_activate_semantics) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) {
        org_to_worker[i] = 0;
    }
    OmMarketSubscription subs[2] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 2, .product_id = 0}
    };
    OmMarketConfig cfg = {
        .max_products = 4,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = org_to_worker,
        .subs = subs,
        .sub_count = 2,
        .expected_orders_per_worker = 8,
        .expected_subscribers_per_product = 2,
        .expected_price_levels = 8,
        .top_levels = 5,
        .dealable = test_marketable,
        .dealable_ctx = NULL
    };

    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketWorker *w = om_market_worker(&market, 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];

    OmWalInsert ins = {
        .order_id = 700,
        .price = 111,
        .volume = 25,
        .vol_remain = 25,
        .org = 1,
        .flags = OM_SIDE_BID,
        .product_id = 0
    };
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_INSERT, &ins), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &ins), 0);

    uint64_t qty = 0;
    ck_assert_int_eq(om_market_worker_get_qty(w, 2, 0, OM_SIDE_BID, 111, &qty), 0);
    ck_assert_uint_eq(qty, 25);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 111, &qty), 0);
    ck_assert_uint_eq(qty, 25);

    OmWalDeactivate deact = {.order_id = 700, .product_id = 0};
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_DEACTIVATE, &deact), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_DEACTIVATE, &deact), 0);

    ck_assert_int_ne(om_market_worker_get_qty(w, 2, 0, OM_SIDE_BID, 111, &qty), 0);
    ck_assert_int_ne(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 111, &qty), 0);

    OmWalActivate act = {.order_id = 700, .product_id = 0};
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_ACTIVATE, &act), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_ACTIVATE, &act), 0);

    /* Current behavior: no restore after deactivate/cancel */
    ck_assert_int_ne(om_market_worker_get_qty(w, 2, 0, OM_SIDE_BID, 111, &qty), 0);
    ck_assert_int_ne(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 111, &qty), 0);

    om_market_destroy(&market);
}
END_TEST

START_TEST(test_market_idempotent_state_transitions) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) {
        org_to_worker[i] = 0;
    }
    OmMarketSubscription subs[2] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 2, .product_id = 0}
    };
    OmMarketConfig cfg = {
        .max_products = 4,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = org_to_worker,
        .subs = subs,
        .sub_count = 2,
        .expected_orders_per_worker = 8,
        .expected_subscribers_per_product = 2,
        .expected_price_levels = 8,
        .top_levels = 5,
        .dealable = test_marketable,
        .dealable_ctx = NULL
    };

    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketWorker *w = om_market_worker(&market, 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];

    OmWalInsert ins = {
        .order_id = 800,
        .price = 77,
        .volume = 20,
        .vol_remain = 20,
        .org = 1,
        .flags = OM_SIDE_BID,
        .product_id = 0
    };
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_INSERT, &ins), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &ins), 0);

    ck_assert_int_eq(om_market_worker_clear_deltas(w, 2, 0, OM_SIDE_BID), 0);
    ck_assert_int_eq(om_market_public_clear_deltas(pub, 0, OM_SIDE_BID), 0);

    OmWalCancel cancel = {.order_id = 800, .product_id = 0};
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_CANCEL, &cancel), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_CANCEL, &cancel), 0);

    ck_assert_int_eq(om_market_worker_clear_deltas(w, 2, 0, OM_SIDE_BID), 0);
    ck_assert_int_eq(om_market_public_clear_deltas(pub, 0, OM_SIDE_BID), 0);

    /* Repeating CANCEL/DEACTIVATE on inactive order should be no-op */
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_CANCEL, &cancel), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_CANCEL, &cancel), 0);
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_DEACTIVATE, &cancel), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_DEACTIVATE, &cancel), 0);

    ck_assert_int_eq(om_market_worker_delta_count(w, 2, 0, OM_SIDE_BID), 0);
    ck_assert_int_eq(om_market_public_delta_count(pub, 0, OM_SIDE_BID), 0);

    /* ACTIVATE on already-active order should also be no-op */
    OmWalInsert ins2 = {
        .order_id = 801,
        .price = 78,
        .volume = 30,
        .vol_remain = 30,
        .org = 1,
        .flags = OM_SIDE_BID,
        .product_id = 0
    };
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_INSERT, &ins2), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &ins2), 0);
    ck_assert_int_eq(om_market_worker_clear_deltas(w, 2, 0, OM_SIDE_BID), 0);
    ck_assert_int_eq(om_market_public_clear_deltas(pub, 0, OM_SIDE_BID), 0);

    OmWalActivate act = {.order_id = 801, .product_id = 0};
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_ACTIVATE, &act), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_ACTIVATE, &act), 0);
    ck_assert_int_eq(om_market_worker_delta_count(w, 2, 0, OM_SIDE_BID), 0);
    ck_assert_int_eq(om_market_public_delta_count(pub, 0, OM_SIDE_BID), 0);

    om_market_destroy(&market);
}
END_TEST

START_TEST(test_market_ring_error_paths) {
    OmMarketRing ring;
    OmMarketRingConfig config_bad_pow2 = {
        .capacity = 7,
        .consumer_count = 1,
        .notify_batch = 1
    };
    OmMarketRingConfig config_bad_cap = {
        .capacity = 0,
        .consumer_count = 1,
        .notify_batch = 1
    };
    OmMarketRingConfig config_bad_consumers = {
        .capacity = 8,
        .consumer_count = 0,
        .notify_batch = 1
    };
    OmMarketRingConfig config_ok = {
        .capacity = 8,
        .consumer_count = 1,
        .notify_batch = 1
    };

    ck_assert_int_eq(om_market_ring_init(NULL, &config_ok), OM_ERR_NULL_PARAM);
    ck_assert_int_eq(om_market_ring_init(&ring, NULL), OM_ERR_NULL_PARAM);
    ck_assert_int_eq(om_market_ring_init(&ring, &config_bad_cap), OM_ERR_INVALID_PARAM);
    ck_assert_int_eq(om_market_ring_init(&ring, &config_bad_consumers), OM_ERR_INVALID_PARAM);
    ck_assert_int_eq(om_market_ring_init(&ring, &config_bad_pow2), OM_ERR_RING_NOT_POW2);

    ck_assert_int_eq(om_market_ring_init(&ring, &config_ok), 0);
    ck_assert_int_eq(om_market_ring_register_consumer(&ring, 1), OM_ERR_RING_CONSUMER_ID);

    void *out = NULL;
    ck_assert_int_eq(om_market_ring_dequeue(&ring, 1, &out), OM_ERR_RING_CONSUMER_ID);
    ck_assert_int_eq(om_market_ring_dequeue(NULL, 0, &out), OM_ERR_NULL_PARAM);
    ck_assert_int_eq(om_market_ring_dequeue(&ring, 0, NULL), OM_ERR_NULL_PARAM);

    void *batch_out[1] = {0};
    ck_assert_int_eq(om_market_ring_dequeue_batch(&ring, 0, batch_out, 0), OM_ERR_INVALID_PARAM);
    ck_assert_int_eq(om_market_ring_dequeue_batch(&ring, 1, batch_out, 1), OM_ERR_RING_CONSUMER_ID);
    ck_assert_int_eq(om_market_ring_dequeue_batch(NULL, 0, batch_out, 1), OM_ERR_NULL_PARAM);

    ck_assert_int_eq(om_market_ring_wait(&ring, 0, 0), OM_ERR_INVALID_PARAM);
    ck_assert_int_eq(om_market_ring_wait(&ring, 1, 1), OM_ERR_RING_CONSUMER_ID);
    ck_assert_int_eq(om_market_ring_wait(NULL, 0, 1), OM_ERR_NULL_PARAM);

    ck_assert_int_eq(om_market_ring_enqueue(NULL, &ring), OM_ERR_NULL_PARAM);
    ck_assert_int_eq(om_market_ring_enqueue(&ring, NULL), OM_ERR_NULL_PARAM);

    om_market_ring_destroy(&ring);
}
END_TEST

START_TEST(test_market_multi_worker_sharding) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) {
        org_to_worker[i] = 0;
    }
    org_to_worker[1] = 0;
    org_to_worker[3] = 0;
    org_to_worker[2] = 1;
    org_to_worker[4] = 1;

    uint32_t product_to_public_worker[4] = {0, 1, 0, 1};
    OmMarketSubscription subs[] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 3, .product_id = 0},
        {.org_id = 2, .product_id = 1},
        {.org_id = 4, .product_id = 1},
    };
    OmMarketConfig cfg = {
        .max_products = 4,
        .worker_count = 2,
        .public_worker_count = 2,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = product_to_public_worker,
        .subs = subs,
        .sub_count = 4,
        .expected_orders_per_worker = 8,
        .expected_subscribers_per_product = 2,
        .expected_price_levels = 8,
        .top_levels = 5,
        .dealable = test_marketable,
        .dealable_ctx = NULL
    };

    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketWorker *w0 = om_market_worker(&market, 0);
    OmMarketWorker *w1 = om_market_worker(&market, 1);
    OmMarketPublicWorker *p0 = &market.public_workers[0];
    OmMarketPublicWorker *p1 = &market.public_workers[1];
    ck_assert_ptr_nonnull(w0);
    ck_assert_ptr_nonnull(w1);

    OmWalInsert a = {
        .order_id = 900,
        .price = 500,
        .volume = 10,
        .vol_remain = 10,
        .org = 1,
        .flags = OM_SIDE_BID,
        .product_id = 0
    };
    OmWalInsert b = {
        .order_id = 901,
        .price = 600,
        .volume = 20,
        .vol_remain = 20,
        .org = 2,
        .flags = OM_SIDE_ASK,
        .product_id = 1
    };

    ck_assert_int_eq(om_market_worker_process(w0, OM_WAL_INSERT, &a), 0);
    ck_assert_int_eq(om_market_public_process(p0, OM_WAL_INSERT, &a), 0);
    ck_assert_int_eq(om_market_worker_process(w1, OM_WAL_INSERT, &b), 0);
    ck_assert_int_eq(om_market_public_process(p1, OM_WAL_INSERT, &b), 0);

    uint64_t qty = 0;
    ck_assert_int_eq(om_market_worker_get_qty(w0, 3, 0, OM_SIDE_BID, 500, &qty), 0);
    ck_assert_uint_eq(qty, 10);
    ck_assert_int_eq(om_market_worker_get_qty(w1, 4, 1, OM_SIDE_ASK, 600, &qty), 0);
    ck_assert_uint_eq(qty, 20);

    ck_assert_int_eq(om_market_public_get_qty(p0, 0, OM_SIDE_BID, 500, &qty), 0);
    ck_assert_uint_eq(qty, 10);
    ck_assert_int_eq(om_market_public_get_qty(p1, 1, OM_SIDE_ASK, 600, &qty), 0);
    ck_assert_uint_eq(qty, 20);

    ck_assert_int_eq(om_market_worker_get_qty(w0, 4, 1, OM_SIDE_ASK, 600, &qty), OM_ERR_NOT_SUBSCRIBED);
    ck_assert_int_eq(om_market_worker_get_qty(w1, 3, 0, OM_SIDE_BID, 500, &qty), OM_ERR_NOT_SUBSCRIBED);

    om_market_destroy(&market);
}
END_TEST

START_TEST(test_market_delta_copy_truncation_and_side_isolation) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) {
        org_to_worker[i] = 0;
    }
    OmMarketSubscription subs[2] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 2, .product_id = 0}
    };
    OmMarketConfig cfg = {
        .max_products = 4,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = org_to_worker,
        .subs = subs,
        .sub_count = 2,
        .expected_orders_per_worker = 8,
        .expected_subscribers_per_product = 2,
        .expected_price_levels = 8,
        .top_levels = 5,
        .dealable = test_marketable,
        .dealable_ctx = NULL
    };

    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketWorker *w = om_market_worker(&market, 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];

    OmWalInsert bid = {
        .order_id = 1001,
        .price = 501,
        .volume = 12,
        .vol_remain = 12,
        .org = 1,
        .flags = OM_SIDE_BID,
        .product_id = 0
    };
    OmWalInsert ask = {
        .order_id = 1002,
        .price = 777,
        .volume = 21,
        .vol_remain = 21,
        .org = 1,
        .flags = OM_SIDE_ASK,
        .product_id = 0
    };
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_INSERT, &bid), 0);
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_INSERT, &ask), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &bid), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &ask), 0);

    OmMarketDelta out[2];
    ck_assert_int_eq(om_market_worker_delta_count(w, 2, 0, OM_SIDE_BID), 1);
    ck_assert_int_eq(om_market_worker_delta_count(w, 2, 0, OM_SIDE_ASK), 1);
    ck_assert_int_eq(om_market_worker_copy_deltas(w, 2, 0, OM_SIDE_BID, out, 1), 1);
    ck_assert_uint_eq(out[0].price, 501);
    ck_assert_int_eq(out[0].delta, 12);
    ck_assert_int_eq(om_market_worker_copy_deltas(w, 2, 0, OM_SIDE_ASK, out, 1), 1);
    ck_assert_uint_eq(out[0].price, 777);
    ck_assert_int_eq(out[0].delta, 21);

    ck_assert_int_eq(om_market_public_delta_count(pub, 0, OM_SIDE_BID), 1);
    ck_assert_int_eq(om_market_public_delta_count(pub, 0, OM_SIDE_ASK), 1);
    ck_assert_int_eq(om_market_public_copy_deltas(pub, 0, OM_SIDE_BID, out, 1), 1);
    ck_assert_uint_eq(out[0].price, 501);
    ck_assert_int_eq(out[0].delta, 12);
    ck_assert_int_eq(om_market_public_copy_deltas(pub, 0, OM_SIDE_ASK, out, 1), 1);
    ck_assert_uint_eq(out[0].price, 777);
    ck_assert_int_eq(out[0].delta, 21);

    om_market_destroy(&market);
}
END_TEST

START_TEST(test_private_copy_full_mixed_ownership_match_cancel) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) {
        org_to_worker[i] = 0;
    }
    OmMarketSubscription subs[3] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 2, .product_id = 0},
        {.org_id = 3, .product_id = 0}
    };
    OmMarketConfig cfg = {
        .max_products = 4,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = org_to_worker,
        .subs = subs,
        .sub_count = 3,
        .expected_orders_per_worker = 8,
        .expected_subscribers_per_product = 3,
        .expected_price_levels = 8,
        .top_levels = 5,
        .dealable = test_marketable,
        .dealable_ctx = NULL
    };

    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketWorker *w = om_market_worker(&market, 0);

    OmWalInsert a = {
        .order_id = 1101,
        .price = 55,
        .volume = 100,
        .vol_remain = 100,
        .org = 1,
        .flags = OM_SIDE_BID,
        .product_id = 0
    };
    OmWalInsert b = {
        .order_id = 1102,
        .price = 55,
        .volume = 80,
        .vol_remain = 80,
        .org = 2,
        .flags = OM_SIDE_BID,
        .product_id = 0
    };
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_INSERT, &a), 0);
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_INSERT, &b), 0);

    OmMarketDelta full[4];
    int n = om_market_worker_copy_full(w, 3, 0, OM_SIDE_BID, full, 4);
    ck_assert_int_eq(n, 1);
    ck_assert_uint_eq(full[0].price, 55);
    ck_assert_int_eq(full[0].delta, 180);

    OmWalMatch m = {.maker_id = 1101, .taker_id = 9999, .price = 55, .volume = 40, .product_id = 0};
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_MATCH, &m), 0);

    n = om_market_worker_copy_full(w, 3, 0, OM_SIDE_BID, full, 4);
    ck_assert_int_eq(n, 1);
    ck_assert_uint_eq(full[0].price, 55);
    ck_assert_int_eq(full[0].delta, 140);

    OmWalCancel c = {.order_id = 1102, .product_id = 0};
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_CANCEL, &c), 0);

    n = om_market_worker_copy_full(w, 3, 0, OM_SIDE_BID, full, 4);
    ck_assert_int_eq(n, 1);
    ck_assert_uint_eq(full[0].price, 55);
    ck_assert_int_eq(full[0].delta, 60);

    uint64_t qty = 0;
    ck_assert_int_eq(om_market_worker_get_qty(w, 3, 0, OM_SIDE_BID, 55, &qty), 0);
    ck_assert_uint_eq(qty, 60);

    om_market_destroy(&market);
}
END_TEST

/* Test slab growth: insert more price levels than initial capacity */
START_TEST(test_market_slab_growth) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) {
        org_to_worker[i] = 0;
    }
    OmMarketSubscription subs[1] = {
        {.org_id = 1, .product_id = 0}
    };
    OmMarketConfig cfg = {
        .max_products = 4,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = org_to_worker,
        .subs = subs,
        .sub_count = 1,
        .expected_orders_per_worker = 4,
        .expected_subscribers_per_product = 1,
        .expected_price_levels = 4,  /* Small initial capacity to trigger growth */
        .top_levels = 100,
        .dealable = test_marketable,
        .dealable_ctx = NULL
    };

    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];

    /* Insert 100 distinct price levels - should trigger slab growth */
    for (uint64_t i = 0; i < 100; i++) {
        OmWalInsert insert = {
            .order_id = i + 1,
            .price = 1000 + i,  /* Distinct prices */
            .volume = 10,
            .vol_remain = 10,
            .org = 1,
            .flags = OM_SIDE_BID,
            .product_id = 0
        };
        ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &insert), 0);
    }

    /* Verify all 100 price levels are tracked */
    uint64_t qty = 0;
    for (uint64_t i = 0; i < 100; i++) {
        ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 1000 + i, &qty), 0);
        ck_assert_uint_eq(qty, 10);
    }

    /* Verify copy_full returns prices in sorted order (descending for bids) */
    OmMarketDelta full[100];
    int n = om_market_public_copy_full(pub, 0, OM_SIDE_BID, full, 100);
    ck_assert_int_eq(n, 100);
    ck_assert_uint_eq(full[0].price, 1099);  /* Best bid (highest) */
    ck_assert_uint_eq(full[99].price, 1000); /* Worst bid (lowest) */
    ck_assert_uint_eq((uintptr_t)pub->slab.slots % 64U, 0U);

    om_market_destroy(&market);
}
END_TEST

/* The dirty work-list must be an exact, duplicate-free view of the per-ladder
 * dirty flags: a flush walks the list instead of probing is_dirty for every
 * subscription, so the two must never disagree. */
START_TEST(test_market_dirty_worklist) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) org_to_worker[i] = 0;
    uint32_t product_to_public[8] = {0};
    OmMarketSubscription subs[4] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 2, .product_id = 0},
        {.org_id = 1, .product_id = 1},
        {.org_id = 3, .product_id = 2},
    };
    OmMarketConfig cfg = {
        .max_products = 8,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = product_to_public,
        .subs = subs,
        .sub_count = 4,
        .expected_orders_per_worker = 8,
        .expected_subscribers_per_product = 2,
        .expected_price_levels = 4,
        .top_levels = 4,
        .dealable = test_marketable,
        .dealable_ctx = NULL,
    };
    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketWorker *w = om_market_worker(&market, 0);
    OmMarketPublicWorker *pw = &market.public_workers[0];

    /* Nothing touched yet: both lists empty. */
    ck_assert_uint_eq(om_market_worker_dirty_count(w), 0);
    ck_assert_uint_eq(om_market_public_dirty_count(pw), 0);

    /* One insert on product 0 fans out to both subscribed orgs (1 and 2). */
    OmWalInsert ins0 = {.order_id = 1, .price = 10, .volume = 5, .vol_remain = 5,
                        .org = 9, .flags = OM_SIDE_BID, .product_id = 0};
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_INSERT, &ins0), 0);
    ck_assert_int_eq(om_market_public_process(pw, OM_WAL_INSERT, &ins0), 0);

    /* A second insert on product 0 must NOT re-list the same ladders (dedup by
     * the clean->dirty edge). */
    OmWalInsert ins0b = {.order_id = 2, .price = 11, .volume = 7, .vol_remain = 7,
                         .org = 9, .flags = OM_SIDE_BID, .product_id = 0};
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_INSERT, &ins0b), 0);
    ck_assert_int_eq(om_market_public_process(pw, OM_WAL_INSERT, &ins0b), 0);

    /* Private: ladders (1,0) and (2,0) are dirty => exactly 2 entries, no dupes. */
    ck_assert_uint_eq(om_market_worker_dirty_count(w), 2);
    for (uint32_t i = 0; i < om_market_worker_dirty_count(w); i++) {
        uint16_t org = 0, prod = 0;
        ck_assert_int_eq(om_market_worker_dirty_at(w, i, &org, &prod), 0);
        ck_assert_uint_eq(prod, 0);
        ck_assert(org == 1 || org == 2);
        ck_assert_int_eq(om_market_worker_is_dirty(w, org, prod), 1);
    }
    /* Public: only product 0 dirty. */
    ck_assert_uint_eq(om_market_public_dirty_count(pw), 1);
    uint16_t pprod = 0xffff;
    ck_assert_int_eq(om_market_public_dirty_at(pw, 0, &pprod), 0);
    ck_assert_uint_eq(pprod, 0);

    /* Clearing a flag does not remove the entry; compaction does. */
    ck_assert_int_eq(om_market_worker_clear_dirty(w, 1, 0), 0);
    ck_assert_uint_eq(om_market_worker_dirty_count(w), 2);   /* stale entry still listed */
    om_market_worker_compact_dirty(w);
    ck_assert_uint_eq(om_market_worker_dirty_count(w), 1);   /* (1,0) dropped */
    uint16_t org = 0, prod = 0;
    ck_assert_int_eq(om_market_worker_dirty_at(w, 0, &org, &prod), 0);
    ck_assert_uint_eq(org, 2);
    ck_assert_uint_eq(prod, 0);

    /* Re-dirtying (1,0) re-lists it exactly once. */
    OmWalInsert ins0c = {.order_id = 3, .price = 12, .volume = 1, .vol_remain = 1,
                         .org = 9, .flags = OM_SIDE_BID, .product_id = 0};
    ck_assert_int_eq(om_market_worker_process(w, OM_WAL_INSERT, &ins0c), 0);
    ck_assert_uint_eq(om_market_worker_dirty_count(w), 2);

    /* Public compaction parity. */
    ck_assert_int_eq(om_market_public_clear_dirty(pw, 0), 0);
    om_market_public_compact_dirty(pw);
    ck_assert_uint_eq(om_market_public_dirty_count(pw), 0);

    /* Out-of-range index is rejected. */
    ck_assert_int_ne(om_market_worker_dirty_at(w, 999, &org, &prod), 0);

    om_market_destroy(&market);
}
END_TEST

Suite* market_suite(void) {
    Suite *s = suite_create("Market");
    TCase *tc_core = tcase_create("core");
    tcase_add_test(tc_core, test_market_struct_sizes);
    tcase_add_test(tc_core, test_market_slab_growth);
    tcase_add_test(tc_core, test_market_ring_basic);
    tcase_add_test(tc_core, test_market_ring_batch);
    tcase_add_test(tc_core, test_market_ring_wait_notify);
    tcase_add_test(tc_core, test_market_worker_dealable);
    tcase_add_test(tc_core, test_market_publish_combos);
    tcase_add_test(tc_core, test_market_multi_org_visibility);
    tcase_add_test(tc_core, test_market_dynamic_ladder);
    tcase_add_test(tc_core, test_market_dynamic_ladder_ask);
    tcase_add_test(tc_core, test_market_dynamic_ladder_match);
    tcase_add_test(tc_core, test_public_same_price_accumulates);
    tcase_add_test(tc_core, test_public_partial_match);
    tcase_add_test(tc_core, test_public_bid_ask_independent);
    tcase_add_test(tc_core, test_public_slot_recycling);
    tcase_add_test(tc_core, test_public_multi_product);
    tcase_add_test(tc_core, test_private_fanout_different_qty);
    tcase_add_test(tc_core, test_private_match_fanout);
    tcase_add_test(tc_core, test_private_copy_full_topn);
    tcase_add_test(tc_core, test_private_slab_growth_fanout);
    tcase_add_test(tc_core, test_private_public_different_views);
    tcase_add_test(tc_core, test_slab_q1_order_after_churn);
    tcase_add_test(tc_core, test_market_deactivate_activate_semantics);
    tcase_add_test(tc_core, test_market_idempotent_state_transitions);
    tcase_add_test(tc_core, test_market_ring_error_paths);
    tcase_add_test(tc_core, test_market_multi_worker_sharding);
    tcase_add_test(tc_core, test_market_delta_copy_truncation_and_side_isolation);
    tcase_add_test(tc_core, test_private_copy_full_mixed_ownership_match_cancel);
    tcase_add_test(tc_core, test_market_dirty_worklist);

    suite_add_tcase(s, tc_core);
    return s;
}
