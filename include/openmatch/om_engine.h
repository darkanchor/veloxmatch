#ifndef OM_ENGINE_H
#define OM_ENGINE_H

#include <stdint.h>
#include <stdbool.h>
#include "om_slab.h"
#include "orderbook.h"
#include "om_wal.h"
#include "om_perf.h"

/**
 * @file om_engine.h
 * @brief Matching engine public API
 * 
 * The engine wraps the orderbook and provides a callback-based interface
 * for matching decisions. Actual matching logic is not implemented here -
 * this is scaffolding for future matching behavior.
 */

/* Forward declarations */
struct OmSlabSlot;
struct OmWal;

/**
 * Matching callback type
 * Called during matching to determine if two orders can match
 * 
 * @param maker The resting order in the book (passive side)
 * @param taker The incoming order (aggressive side)
 * @param user_ctx User-provided context pointer
 * @return Volume that can be matched (0 = no match, >0 = match volume)
 * 
 * Example use cases:
 * - Reject self-matching (same org)
 * - Implement custom matching rules
 * - Rate limiting or circuit breakers
 */
typedef uint64_t (*OmCanMatchFn)(const OmSlabSlot *maker, 
                                  const OmSlabSlot *taker, 
                                  void *user_ctx);

/* Reserved OmCanMatchFn action sentinels. Normal callbacks return a matchable
 * quantity. These high values request a structural self-trade action instead. */
#define OM_CAN_MATCH_CANCEL_MAKER (UINT64_MAX - 1ULL)
#define OM_CAN_MATCH_CANCEL_BOTH  (UINT64_MAX - 2ULL)
#define OM_CAN_MATCH_DECREMENT_CANCEL (UINT64_MAX - 3ULL)

/**
 * Match callback type
 * Called when a trade is executed for a single order (maker or taker)
 *
 * @param order The order being filled
 * @param price Execution price
 * @param qty Execution quantity for this callback
 * @param user_ctx User-provided context pointer
 */
typedef void (*OmOnMatchFn)(const OmSlabSlot *order,
                            uint64_t price,
                            uint64_t qty,
                            void *user_ctx);

/**
 * Deal callback type
 * Called once per trade with both maker and taker
 *
 * @param maker The resting order in the book (passive side)
 * @param taker The incoming order (aggressive side)
 * @param price Execution price
 * @param qty Execution quantity
 * @param user_ctx User-provided context pointer
 */
typedef void (*OmOnDealFn)(const OmSlabSlot *maker,
                           const OmSlabSlot *taker,
                           uint64_t price,
                           uint64_t qty,
                           void *user_ctx);

/**
 * Booked callback type
 * Called when a taker order rests on the book
 *
 * @param order The order that was booked
 * @param user_ctx User-provided context pointer
 */
typedef void (*OmOnBookedFn)(const OmSlabSlot *order, void *user_ctx);

/**
 * Filled callback type
 * Called when a maker order is fully filled and removed from the book
 *
 * @param order The order that was fully filled
 * @param user_ctx User-provided context pointer
 */
typedef void (*OmOnFilledFn)(const OmSlabSlot *order, void *user_ctx);

/**
 * Cancel callback type
 * Called when a maker order is voluntarily cancelled and removed from the book
 *
 * @param order The order that was cancelled
 * @param user_ctx User-provided context pointer
 */
typedef void (*OmOnCancelFn)(const OmSlabSlot *order, void *user_ctx);

/**
 * Decrement-cancel callback type
 * Called when self-trade prevention decrements maker/taker instead of trading.
 *
 * @param maker The resting order after its remaining quantity was decremented
 * @param taker The incoming order after its remaining quantity was decremented
 * @param qty Quantity removed from both orders
 * @param user_ctx User-provided context pointer
 */
typedef void (*OmOnDecrementCancelFn)(const OmSlabSlot *maker,
                                      const OmSlabSlot *taker,
                                      uint64_t qty,
                                      void *user_ctx);

/**
 * Pre-booked callback type
 * Called before booking a taker order that has remaining quantity
 * Return true to allow booking, false to cancel remaining quantity.
 *
 * @param order The taker order to be booked
 * @param user_ctx User-provided context pointer
 * @return true to book, false to cancel remaining qty
 */
typedef bool (*OmPreBookedFn)(const OmSlabSlot *order, void *user_ctx);

/**
 * Engine callbacks configuration
 * Contains all user-defined callbacks for engine behavior
 */
typedef struct OmEngineCallbacks {
    OmCanMatchFn can_match;     /**< Optional matching predicate callback */
    OmOnMatchFn on_match;        /**< Optional match notification callback */
    OmOnDealFn on_deal;           /**< Optional deal notification callback */
    OmOnBookedFn on_booked;       /**< Optional booked notification callback */
    OmOnFilledFn on_filled;       /**< Optional filled notification callback */
    OmOnCancelFn on_cancel;        /**< Optional cancel notification callback */
    OmOnDecrementCancelFn on_decrement_cancel; /**< Optional decrement-cancel callback */
    OmPreBookedFn pre_booked;      /**< Optional pre-booking decision callback */
    void *user_ctx;             /**< User context passed to callbacks */
} OmEngineCallbacks;

/**
 * Engine configuration
 * Combines slab, WAL, and callback configuration
 */
typedef struct OmEngineConfig {
    OmSlabConfig slab;          /**< Slab allocator configuration (required) */
    OmWalConfig *wal;           /**< Optional WAL configuration (NULL to disable) */
    uint32_t max_products;       /**< Number of products to allocate */
    uint32_t max_org;            /**< Number of orgs to allocate per product */
    uint32_t hashmap_initial_cap; /**< Initial hashmap capacity (0 = default) */
    OmEngineCallbacks callbacks; /**< Callback configuration */
    const OmPerfConfig *perf;     /**< Optional perf preset (overrides sizes/flags) */
} OmEngineConfig;

/**
 * Matching engine context
 * Wraps the orderbook and stores callback configuration
 */
typedef struct OmEngine {
    OmOrderbookContext orderbook; /**< Internal orderbook (slab, products, hashmap) */
    OmEngineCallbacks callbacks;   /**< Stored callback configuration */
    struct OmWal *wal;            /**< WAL pointer (owned if config provided, NULL otherwise) */
    bool wal_owned;               /**< true if engine allocated WAL internally */
} OmEngine;

/**
 * Initialize matching engine
 * 
 * Initializes the orderbook with the provided slab and WAL configuration.
 * Stores callback pointers for later use during matching.
 * 
 * @param engine Engine context to initialize
 * @param config Engine configuration (slab, optional WAL, callbacks)
 * @return 0 on success, negative on error (see om_error.h)
 * 
 * Error codes:
 *   OM_ERR_NULL_PARAM: Invalid parameters (NULL engine or config)
 *   OM_ERR_INVALID_PARAM: Invalid configuration values
 *   OM_ERR_ENGINE_WAL_INIT: WAL initialization failed
 *   OM_ERR_ENGINE_OB_INIT: Orderbook initialization failed
 */
int om_engine_init(OmEngine *engine, const OmEngineConfig *config);

/**
 * Initialize matching engine using a performance preset
 *
 * @param engine Engine context to initialize
 * @param config Engine configuration (callbacks + optional WAL)
 * @param perf Performance preset to apply
 * @return 0 on success, negative on error
 */
int om_engine_init_perf(OmEngine *engine, const OmEngineConfig *config, const OmPerfConfig *perf);

/**
 * Destroy matching engine and free all resources
 * 
 * Destroys the orderbook, closes the WAL (if owned), and clears callback pointers.
 * Safe to call on a zeroed or partially initialized engine.
 * 
 * @param engine Engine context to destroy
 */
void om_engine_destroy(OmEngine *engine);

/**
 * Get the orderbook context from engine
 * 
 * Useful for direct orderbook operations (insert, cancel, query).
 * 
 * @param engine Engine context
 * @return Pointer to orderbook context, or NULL if engine is NULL
 */
static inline OmOrderbookContext *om_engine_get_orderbook(OmEngine *engine) {
    return engine ? &engine->orderbook : NULL;
}

/**
 * Get the WAL context from engine
 * 
 * @param engine Engine context
 * @return Pointer to WAL, or NULL if WAL disabled or engine is NULL
 */
static inline struct OmWal *om_engine_get_wal(OmEngine *engine) {
    return engine ? engine->wal : NULL;
}

/**
 * Check if a matching callback is configured
 * 
 * @param engine Engine context
 * @return true if can_match callback is set
 */
static inline bool om_engine_has_can_match(const OmEngine *engine) {
    return engine && engine->callbacks.can_match != NULL;
}

/**
 * Check if a match callback is configured
 *
 * @param engine Engine context
 * @return true if on_match callback is set
 */
static inline bool om_engine_has_on_match(const OmEngine *engine) {
    return engine && engine->callbacks.on_match != NULL;
}

/**
 * Check if a deal callback is configured
 *
 * @param engine Engine context
 * @return true if on_deal callback is set
 */
static inline bool om_engine_has_on_deal(const OmEngine *engine) {
    return engine && engine->callbacks.on_deal != NULL;
}

/**
 * Check if a booked callback is configured
 *
 * @param engine Engine context
 * @return true if on_booked callback is set
 */
static inline bool om_engine_has_on_booked(const OmEngine *engine) {
    return engine && engine->callbacks.on_booked != NULL;
}

/**
 * Check if a filled callback is configured
 *
 * @param engine Engine context
 * @return true if on_filled callback is set
 */
static inline bool om_engine_has_on_filled(const OmEngine *engine) {
    return engine && engine->callbacks.on_filled != NULL;
}

/**
 * Check if a cancel callback is configured
 *
 * @param engine Engine context
 * @return true if on_cancel callback is set
 */
static inline bool om_engine_has_on_cancel(const OmEngine *engine) {
    return engine && engine->callbacks.on_cancel != NULL;
}

/**
 * Check if a pre-booked callback is configured
 *
 * @param engine Engine context
 * @return true if pre_booked callback is set
 */
static inline bool om_engine_has_pre_booked(const OmEngine *engine) {
    return engine && engine->callbacks.pre_booked != NULL;
}

/**
 * Match an incoming order against the orderbook
 *
 * @param engine Engine context
 * @param product_id Product ID
 * @param taker Incoming order slot
 * @return 0 on success, negative on error
 */
int om_engine_match(OmEngine *engine, uint16_t product_id, OmSlabSlot *taker);

/**
 * Cancel a single order by order ID
 *
 * @param engine Engine context
 * @param order_id Order ID to cancel
 * @return true if cancelled, false if not found
 */
bool om_engine_cancel(OmEngine *engine, uint32_t order_id);

/**
 * Deactivate a resting order and remove it from the book without freeing it
 *
 * @param engine Engine context
 * @param order_id Order ID to deactivate
 * @return true if deactivated, false if not found or not active
 */
bool om_engine_deactivate(OmEngine *engine, uint32_t order_id);

/**
 * Activate a previously deactivated order and re-run matching as taker
 *
 * @param engine Engine context
 * @param order_id Order ID to activate
 * @return true if activated, false if not found or not deactivated
 */
bool om_engine_activate(OmEngine *engine, uint32_t order_id);

/**
 * Cancel all orders for an org within a product
 *
 * @param engine Engine context
 * @param product_id Product ID
 * @param org_id Organization ID
 * @return Number of orders cancelled
 */
uint32_t om_engine_cancel_org_product(OmEngine *engine, uint16_t product_id, uint16_t org_id);

/**
 * Cancel all orders for an org across all products
 *
 * @param engine Engine context
 * @param org_id Organization ID
 * @return Number of orders cancelled
 */
uint32_t om_engine_cancel_org_all(OmEngine *engine, uint16_t org_id);

/**
 * Cancel all orders for a product on one side
 *
 * @param engine Engine context
 * @param product_id Product ID
 * @param is_bid true for bid side, false for ask side
 * @return Number of orders cancelled
 */
uint32_t om_engine_cancel_product_side(OmEngine *engine, uint16_t product_id, bool is_bid);

/**
 * Cancel all orders for a product on both sides
 *
 * @param engine Engine context
 * @param product_id Product ID
 * @return Number of orders cancelled
 */
uint32_t om_engine_cancel_product(OmEngine *engine, uint16_t product_id);

#endif /* OM_ENGINE_H */
