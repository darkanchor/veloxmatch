#include <check.h>
#include <stdint.h>
#include "openmatch/om_engine.h"

typedef struct TestMatchCtx {
    uint64_t can_match_calls;
    uint64_t on_match_calls;
    uint64_t on_deal_calls;
    uint32_t dealt_makers[64];
    uint64_t dealt_prices[64];
    uint64_t dealt_quantities[64];
    uint64_t on_booked_calls;
    uint64_t on_filled_calls;
    uint64_t on_cancel_calls;
    uint64_t can_match_cap;
    bool can_match_zero;
    bool can_match_skip_once;
    bool pre_booked_allow;
} TestMatchCtx;

static uint64_t test_can_match(const OmSlabSlot *maker, const OmSlabSlot *taker, void *user_ctx)
{
    (void)maker;
    (void)taker;
    TestMatchCtx *ctx = (TestMatchCtx *)user_ctx;
    ctx->can_match_calls++;
    if (ctx->can_match_skip_once) {
        ctx->can_match_skip_once = false;
        return 0;
    }
    if (ctx->can_match_zero) {
        return 0;
    }
    if (ctx->can_match_cap != 0) {
        return ctx->can_match_cap;
    }
    return UINT64_MAX;
}

static void test_on_match(const OmSlabSlot *order, uint64_t price, uint64_t qty, void *user_ctx)
{
    (void)order;
    (void)price;
    (void)qty;
    TestMatchCtx *ctx = (TestMatchCtx *)user_ctx;
    ctx->on_match_calls++;
}

static void test_on_deal(const OmSlabSlot *maker, const OmSlabSlot *taker,
                         uint64_t price, uint64_t qty, void *user_ctx)
{
    (void)maker;
    (void)taker;
    (void)price;
    (void)qty;
    TestMatchCtx *ctx = (TestMatchCtx *)user_ctx;
    ck_assert_uint_lt(ctx->on_deal_calls, 64);
    ctx->dealt_makers[ctx->on_deal_calls] = maker ? maker->order_id : 0;
    ctx->dealt_prices[ctx->on_deal_calls] = price;
    ctx->dealt_quantities[ctx->on_deal_calls] = qty;
    ctx->on_deal_calls++;
}

static void test_on_booked(const OmSlabSlot *order, void *user_ctx)
{
    (void)order;
    TestMatchCtx *ctx = (TestMatchCtx *)user_ctx;
    ctx->on_booked_calls++;
}

static void test_on_filled(const OmSlabSlot *order, void *user_ctx)
{
    (void)order;
    TestMatchCtx *ctx = (TestMatchCtx *)user_ctx;
    ctx->on_filled_calls++;
}

static void test_on_cancel(const OmSlabSlot *order, void *user_ctx)
{
    (void)order;
    TestMatchCtx *ctx = (TestMatchCtx *)user_ctx;
    ctx->on_cancel_calls++;
}

static bool test_pre_booked(const OmSlabSlot *order, void *user_ctx)
{
    (void)order;
    TestMatchCtx *ctx = (TestMatchCtx *)user_ctx;
    return ctx->pre_booked_allow;
}

static void init_engine_with_ctx(OmEngine *engine, TestMatchCtx *ctx)
{
    OmEngineConfig config = {
        .slab = {
            .user_data_size = 64,
            .aux_data_size = 128,
            .total_slots = 1000
        },
        .wal = NULL,
        .max_products = 10,
        .max_org = 100,
        .hashmap_initial_cap = 0,
        .perf = NULL,
        .callbacks = {
            .can_match = test_can_match,
            .on_match = test_on_match,
            .on_deal = test_on_deal,
            .on_booked = test_on_booked,
            .on_filled = test_on_filled,
            .on_cancel = test_on_cancel,
            .pre_booked = test_pre_booked,
            .user_ctx = ctx
        }
    };

    ck_assert_int_eq(om_engine_init(engine, &config), 0);
}

static OmSlabSlot *make_order(OmEngine *engine, uint64_t price, uint64_t volume, uint16_t flags)
{
    OmSlabSlot *order = om_slab_alloc(&engine->orderbook.slab);
    ck_assert_ptr_nonnull(order);
    om_slot_set_order_id(order, om_slab_next_order_id(&engine->orderbook.slab));
    om_slot_set_price(order, price);
    om_slot_set_volume(order, volume);
    om_slot_set_volume_remain(order, volume);
    om_slot_set_flags(order, flags);
    om_slot_set_org(order, 1);
    return order;
}

START_TEST(test_engine_init_callbacks)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);
    ck_assert_ptr_nonnull(om_engine_get_orderbook(&engine));
    ck_assert(om_engine_has_can_match(&engine));

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_cancel_product_side)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *bid = om_slab_alloc(&engine.orderbook.slab);
    OmSlabSlot *ask = om_slab_alloc(&engine.orderbook.slab);
    ck_assert_ptr_nonnull(bid);
    ck_assert_ptr_nonnull(ask);

    om_slot_set_order_id(bid, om_slab_next_order_id(&engine.orderbook.slab));
    om_slot_set_price(bid, 10000);
    om_slot_set_volume(bid, 10);
    om_slot_set_volume_remain(bid, 10);
    om_slot_set_flags(bid, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(bid, 1);

    om_slot_set_order_id(ask, om_slab_next_order_id(&engine.orderbook.slab));
    om_slot_set_price(ask, 10100);
    om_slot_set_volume(ask, 5);
    om_slot_set_volume_remain(ask, 5);
    om_slot_set_flags(ask, OM_SIDE_ASK | OM_TYPE_LIMIT);
    om_slot_set_org(ask, 1);

    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, bid), 0);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, ask), 0);

    ck_assert_uint_eq(om_engine_cancel_product_side(&engine, 0, true), 1);
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, bid->order_id));
    ck_assert_ptr_nonnull(om_orderbook_get_slot_by_id(&engine.orderbook, ask->order_id));
    ck_assert_uint_eq(ctx.on_cancel_calls, 1);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_cancel_product)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *bid = om_slab_alloc(&engine.orderbook.slab);
    OmSlabSlot *ask = om_slab_alloc(&engine.orderbook.slab);
    ck_assert_ptr_nonnull(bid);
    ck_assert_ptr_nonnull(ask);

    om_slot_set_order_id(bid, om_slab_next_order_id(&engine.orderbook.slab));
    om_slot_set_price(bid, 10000);
    om_slot_set_volume(bid, 10);
    om_slot_set_volume_remain(bid, 10);
    om_slot_set_flags(bid, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(bid, 1);

    om_slot_set_order_id(ask, om_slab_next_order_id(&engine.orderbook.slab));
    om_slot_set_price(ask, 10100);
    om_slot_set_volume(ask, 5);
    om_slot_set_volume_remain(ask, 5);
    om_slot_set_flags(ask, OM_SIDE_ASK | OM_TYPE_LIMIT);
    om_slot_set_org(ask, 1);

    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, bid), 0);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, ask), 0);

    ck_assert_uint_eq(om_engine_cancel_product(&engine, 0), 2);
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, bid->order_id));
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, ask->order_id));
    ck_assert_uint_eq(ctx.on_cancel_calls, 2);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_callback_context)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    engine.callbacks.can_match(NULL, NULL, &ctx);
    engine.callbacks.on_match(NULL, 0, 0, &ctx);
    engine.callbacks.on_deal(NULL, NULL, 0, 0, &ctx);
    engine.callbacks.on_booked(NULL, &ctx);
    engine.callbacks.on_filled(NULL, &ctx);
    engine.callbacks.on_cancel(NULL, &ctx);
    ck_assert_uint_eq(ctx.can_match_calls, 1);
    ck_assert_uint_eq(ctx.on_match_calls, 1);
    ck_assert_uint_eq(ctx.on_deal_calls, 1);
    ck_assert_uint_eq(ctx.on_booked_calls, 1);
    ck_assert_uint_eq(ctx.on_filled_calls, 1);
    ck_assert_uint_eq(ctx.on_cancel_calls, 1);

    ck_assert(engine.callbacks.pre_booked(NULL, engine.callbacks.user_ctx));

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_pre_booked_cancel)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = false;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *taker = make_order(&engine, 10000, 10, OM_SIDE_BID | OM_TYPE_LIMIT);

    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, taker->order_id));
    ck_assert_uint_eq(ctx.on_cancel_calls, 1);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_cancel_single)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *order = make_order(&engine, 10000, 10, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, order), 0);

    ck_assert(om_engine_cancel(&engine, order->order_id));
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, order->order_id));
    ck_assert_uint_eq(ctx.on_cancel_calls, 1);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_full_fill_single)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10000, 10, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    OmSlabSlot *taker = make_order(&engine, 10100, 10, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_deal_calls, 1);
    ck_assert_uint_eq(ctx.on_match_calls, 2);
    ck_assert_uint_eq(ctx.on_filled_calls, 1);
    ck_assert_uint_eq(ctx.on_booked_calls, 0);
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, maker->order_id));

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_partial_fill_maker_remaining)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10000, 10, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    OmSlabSlot *taker = make_order(&engine, 10100, 5, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    OmSlabSlot *maker_left = om_orderbook_get_slot_by_id(&engine.orderbook, maker->order_id);
    ck_assert_ptr_nonnull(maker_left);
    ck_assert_uint_eq(maker_left->volume_remain, 5);
    ck_assert_uint_eq(ctx.on_filled_calls, 0);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_partial_fill_taker_booked)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    OmSlabSlot *taker = make_order(&engine, 10100, 10, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_booked_calls, 1);
    ck_assert_ptr_nonnull(om_orderbook_get_slot_by_id(&engine.orderbook, taker->order_id));

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_price_not_cross)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10050, 10, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    OmSlabSlot *taker = make_order(&engine, 10000, 10, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_deal_calls, 0);
    ck_assert_uint_eq(ctx.on_booked_calls, 1);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_multi_maker_levels)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker1 = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    OmSlabSlot *maker2 = make_order(&engine, 10100, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker1), 0);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker2), 0);

    OmSlabSlot *taker = make_order(&engine, 10100, 10, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_deal_calls, 2);
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, maker1->order_id));
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, maker2->order_id));

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_same_price_fifo)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker1 = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    OmSlabSlot *maker2 = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker1), 0);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker2), 0);

    OmSlabSlot *taker = make_order(&engine, 10000, 10, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, maker1->order_id));
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, maker2->order_id));

    om_engine_destroy(&engine);
}
END_TEST

/* Compare execution order with a simple price/arrival reference on both sides. */
START_TEST(test_engine_strict_price_time_after_partial_and_cancel)
{
    for (unsigned bid = 0; bid < 2; bid++) {
        OmEngine engine;
        TestMatchCtx ctx = {0};
        ctx.pre_booked_allow = true;
        init_engine_with_ctx(&engine, &ctx);
        uint64_t prices[10] = {103, 100, 102, 101, 100, 99, 104, 100, 100, 0};
        uint64_t remaining[10] = {5, 5, 5, 5, 5, 5, 5, 5, 5, 1};
        uint32_t ids[10];
        const uint16_t maker_flags = (bid ? OM_SIDE_BID : OM_SIDE_ASK) | OM_TYPE_LIMIT;
        const uint16_t taker_flags = (bid ? OM_SIDE_ASK : OM_SIDE_BID) | OM_TYPE_LIMIT;
        const uint64_t limit = bid ? 99 : 104;
        for (unsigned i = 0; i < 8; i++) {
            OmSlabSlot *maker = make_order(&engine, prices[i], remaining[i], maker_flags);
            ids[i] = maker->order_id;
            ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);
        }
        /* Cancel a FIFO head, then append a newer order at that same price. */
        ck_assert(om_engine_cancel(&engine, ids[1]));
        remaining[1] = 0;
        OmSlabSlot *replacement = make_order(&engine, prices[8], remaining[8], maker_flags);
        ids[8] = replacement->order_id;
        ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, replacement), 0);
        unsigned best = bid ? 6 : 5;
        OmSlabSlot *partial = make_order(&engine, limit, 2, taker_flags);
        ck_assert_int_eq(om_engine_match(&engine, 0, partial), 0);
        ck_assert_uint_eq(ctx.on_deal_calls, 1);
        ck_assert_uint_eq(ctx.dealt_makers[0], ids[best]);
        ck_assert_uint_eq(ctx.dealt_prices[0], prices[best]);
        ck_assert_uint_eq(ctx.dealt_quantities[0], 2);
        remaining[best] -= 2;
        om_slab_free(&engine.orderbook.slab, partial);
        /* A later arrival must remain behind the partially filled oldest order. */
        prices[9] = prices[best];
        OmSlabSlot *late = make_order(&engine, prices[9], remaining[9], maker_flags);
        ids[9] = late->order_id;
        ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, late), 0);
        uint64_t total = 0;
        for (unsigned i = 0; i < 10; i++) total += remaining[i];
        ctx.on_deal_calls = 0;
        OmSlabSlot *sweep = make_order(&engine, limit, total, taker_flags);
        ck_assert_int_eq(om_engine_match(&engine, 0, sweep), 0);
        ck_assert_uint_eq(ctx.on_deal_calls, 9);
        for (unsigned execution = 0; execution < 9; execution++) {
            unsigned next = 10;
            for (unsigned i = 0; i < 10; i++) {
                if (!remaining[i]) continue;
                if (next == 10 || (bid ? prices[i] > prices[next] : prices[i] < prices[next]))
                    next = i;
                /* Equal prices deliberately retain the earlier insertion index. */
            }
            ck_assert_uint_lt(next, 10);
            ck_assert_uint_eq(ctx.dealt_makers[execution], ids[next]);
            ck_assert_uint_eq(ctx.dealt_prices[execution], prices[next]);
            ck_assert_uint_eq(ctx.dealt_quantities[execution], remaining[next]);
            remaining[next] = 0;
        }
        ck_assert_uint_eq(sweep->volume_remain, 0);
        om_slab_free(&engine.orderbook.slab, sweep);
        om_engine_destroy(&engine);
    }
}
END_TEST

START_TEST(test_engine_match_can_match_cap)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    ctx.can_match_cap = 3;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10000, 10, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    OmSlabSlot *taker = make_order(&engine, 10100, 3, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    OmSlabSlot *maker_left = om_orderbook_get_slot_by_id(&engine.orderbook, maker->order_id);
    ck_assert_ptr_nonnull(maker_left);
    ck_assert_uint_eq(maker_left->volume_remain, 7);
    ck_assert_uint_eq(ctx.on_deal_calls, 1);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_can_match_zero)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    ctx.can_match_zero = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10000, 10, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    OmSlabSlot *taker = make_order(&engine, 10100, 10, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_deal_calls, 0);
    ck_assert_uint_eq(ctx.on_booked_calls, 1);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_can_match_skip_best)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker1 = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    OmSlabSlot *maker2 = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker1), 0);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker2), 0);

    OmSlabSlot *taker = make_order(&engine, 10000, 5, OM_SIDE_BID | OM_TYPE_LIMIT);

    /* First maker skipped, second allowed */
    ctx.can_match_skip_once = true;
    ctx.can_match_calls = 0;

    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_deal_calls, 1);
    ck_assert_ptr_nonnull(om_orderbook_get_slot_by_id(&engine.orderbook, maker1->order_id));
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, maker2->order_id));

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_can_match_skip_level_then_book)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker1 = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker1), 0);

    OmSlabSlot *maker2 = make_order(&engine, 10100, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker2), 0);

    OmSlabSlot *taker = make_order(&engine, 10100, 5, OM_SIDE_BID | OM_TYPE_LIMIT);

    ctx.can_match_zero = true;
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_deal_calls, 0);
    ck_assert_uint_eq(ctx.on_booked_calls, 1);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_pre_booked_false_cancels_remaining)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = false;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    OmSlabSlot *taker = make_order(&engine, 10100, 10, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_cancel_calls, 1);
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, taker->order_id));

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_multi_product_isolated)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 1, maker), 0);

    OmSlabSlot *taker = make_order(&engine, 10100, 5, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_deal_calls, 0);
    ck_assert_uint_eq(ctx.on_booked_calls, 1);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_bid_vs_bid_no_cross)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10000, 5, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    OmSlabSlot *taker = make_order(&engine, 10000, 5, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_deal_calls, 0);
    ck_assert_uint_eq(ctx.on_booked_calls, 1);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_deactivate_activate)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    ck_assert(om_engine_deactivate(&engine, maker->order_id));
    ck_assert_ptr_nonnull(om_orderbook_get_slot_by_id(&engine.orderbook, maker->order_id));
    ck_assert_uint_eq((maker->flags & OM_STATUS_MASK), OM_STATUS_DEACTIVATED);

    ck_assert(om_engine_activate(&engine, maker->order_id));
    ck_assert_ptr_nonnull(om_orderbook_get_slot_by_id(&engine.orderbook, maker->order_id));

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_cancel_org_product)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *o1 = om_slab_alloc(&engine.orderbook.slab);
    OmSlabSlot *o2 = om_slab_alloc(&engine.orderbook.slab);
    OmSlabSlot *o3 = om_slab_alloc(&engine.orderbook.slab);

    om_slot_set_order_id(o1, om_slab_next_order_id(&engine.orderbook.slab));
    om_slot_set_price(o1, 10000);
    om_slot_set_volume(o1, 10);
    om_slot_set_volume_remain(o1, 10);
    om_slot_set_flags(o1, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(o1, 1);

    om_slot_set_order_id(o2, om_slab_next_order_id(&engine.orderbook.slab));
    om_slot_set_price(o2, 10000);
    om_slot_set_volume(o2, 10);
    om_slot_set_volume_remain(o2, 10);
    om_slot_set_flags(o2, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(o2, 1);

    om_slot_set_order_id(o3, om_slab_next_order_id(&engine.orderbook.slab));
    om_slot_set_price(o3, 10000);
    om_slot_set_volume(o3, 10);
    om_slot_set_volume_remain(o3, 10);
    om_slot_set_flags(o3, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(o3, 2);

    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, o1), 0);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, o2), 0);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, o3), 0);

    ck_assert_uint_eq(om_engine_cancel_org_product(&engine, 0, 1), 2);
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, o1->order_id));
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, o2->order_id));
    ck_assert_ptr_nonnull(om_orderbook_get_slot_by_id(&engine.orderbook, o3->order_id));

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_cancel_org_all)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *o1 = om_slab_alloc(&engine.orderbook.slab);
    OmSlabSlot *o2 = om_slab_alloc(&engine.orderbook.slab);

    om_slot_set_order_id(o1, om_slab_next_order_id(&engine.orderbook.slab));
    om_slot_set_price(o1, 10000);
    om_slot_set_volume(o1, 10);
    om_slot_set_volume_remain(o1, 10);
    om_slot_set_flags(o1, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(o1, 3);

    om_slot_set_order_id(o2, om_slab_next_order_id(&engine.orderbook.slab));
    om_slot_set_price(o2, 10100);
    om_slot_set_volume(o2, 10);
    om_slot_set_volume_remain(o2, 10);
    om_slot_set_flags(o2, OM_SIDE_ASK | OM_TYPE_LIMIT);
    om_slot_set_org(o2, 3);

    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, o1), 0);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 1, o2), 0);

    ck_assert_uint_eq(om_engine_cancel_org_all(&engine, 3), 2);
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, o1->order_id));
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, o2->order_id));

    om_engine_destroy(&engine);
}
END_TEST

Suite *engine_suite(void)
{
    Suite *s = suite_create("Engine");
    TCase *tc_core = tcase_create("Core");

    tcase_add_test(tc_core, test_engine_init_callbacks);
    tcase_add_test(tc_core, test_engine_callback_context);
    tcase_add_test(tc_core, test_engine_match_pre_booked_cancel);
    tcase_add_test(tc_core, test_engine_cancel_single);
    tcase_add_test(tc_core, test_engine_match_full_fill_single);
    tcase_add_test(tc_core, test_engine_match_partial_fill_maker_remaining);
    tcase_add_test(tc_core, test_engine_match_partial_fill_taker_booked);
    tcase_add_test(tc_core, test_engine_match_price_not_cross);
    tcase_add_test(tc_core, test_engine_match_multi_maker_levels);
    tcase_add_test(tc_core, test_engine_match_same_price_fifo);
    tcase_add_test(tc_core, test_engine_strict_price_time_after_partial_and_cancel);
    tcase_add_test(tc_core, test_engine_match_can_match_cap);
    tcase_add_test(tc_core, test_engine_match_can_match_zero);
    tcase_add_test(tc_core, test_engine_match_can_match_skip_best);
    tcase_add_test(tc_core, test_engine_match_can_match_skip_level_then_book);
    tcase_add_test(tc_core, test_engine_match_pre_booked_false_cancels_remaining);
    tcase_add_test(tc_core, test_engine_match_multi_product_isolated);
    tcase_add_test(tc_core, test_engine_match_bid_vs_bid_no_cross);
    tcase_add_test(tc_core, test_engine_deactivate_activate);
    tcase_add_test(tc_core, test_engine_cancel_org_product);
    tcase_add_test(tc_core, test_engine_cancel_org_all);
    tcase_add_test(tc_core, test_engine_cancel_product_side);
    tcase_add_test(tc_core, test_engine_cancel_product);

    suite_add_tcase(s, tc_core);
    return s;
}
