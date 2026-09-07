#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "../include/openmatch/om_slab.h"
#include "../include/openmatch/om_error.h"

static inline size_t align_up(size_t size, size_t align) {
    return (size + align - 1) & ~(align - 1);
}

/* Helper: Get slot pointer from index in slab A */
static inline OmSlabSlot *idx_to_slot_a(const OmSlabA *slab_a, uint32_t idx) {
    if (idx == OM_SLOT_IDX_NULL || idx >= slab_a->capacity) return NULL;
    return (OmSlabSlot *)(slab_a->memory + idx * slab_a->slot_size);
}

/* Helper: Get index from slot pointer for slab A */
static inline uint32_t slot_to_idx_a(const OmSlabA *slab_a, const OmSlabSlot *slot) {
    ptrdiff_t offset = (const uint8_t *)slot - slab_a->memory;
    if (offset < 0 || offset % slab_a->slot_size != 0) return OM_SLOT_IDX_NULL;
    return (uint32_t)(offset / slab_a->slot_size);
}

/* Get slot index within slab A */
uint32_t om_slot_get_idx(const OmDualSlab *slab, const OmSlabSlot *slot) {
    return slot_to_idx_a(&slab->slab_a, slot);
}

/* Get slot from index in slab A */
OmSlabSlot *om_slot_from_idx(const OmDualSlab *slab, uint32_t idx) {
    if (idx == OM_SLOT_IDX_NULL) return NULL;
    return idx_to_slot_a(&slab->slab_a, idx);
}

int om_slab_init(OmDualSlab *slab, const OmSlabConfig *config) {
    if (!slab || !config) {
        return OM_ERR_NULL_PARAM;
    }
    if (config->total_slots == 0) {
        return OM_ERR_INVALID_PARAM;
    }

    memset(slab, 0, sizeof(OmDualSlab));
    slab->config = *config;

    /* Slot size = mandatory fields + queue nodes + user data */
    size_t slot_size = sizeof(OmSlabSlot) + align_up(config->user_data_size, 8);
    slab->slab_a.slot_size = slot_size;
    slab->slab_a.capacity = config->total_slots;
    slab->slab_a.used = 0;

    size_t total_a_size = slot_size * config->total_slots;
    slab->slab_a.memory = calloc(1, total_a_size);
    if (!slab->slab_a.memory) {
        return OM_ERR_ALLOC_FAILED;
    }

    /* Build free list - Q0 is reserved for internal slab use */
    slab->slab_a.free_list_idx = OM_SLOT_IDX_NULL;
    for (uint32_t i = 0; i < config->total_slots; i++) {
        OmSlabSlot *slot = (OmSlabSlot *)(slab->slab_a.memory + i * slot_size);
        /* Initialize all queue nodes to NULL */
        for (int q = 0; q < OM_MAX_QUEUES; q++) {
            slot->queue_nodes[q].next_idx = OM_SLOT_IDX_NULL;
            slot->queue_nodes[q].prev_idx = OM_SLOT_IDX_NULL;
        }
        /* Use Q0 for internal free list */
        slot->queue_nodes[OM_Q0_INTERNAL_FREE].next_idx = slab->slab_a.free_list_idx;
        slab->slab_a.free_list_idx = i;
    }

    /* Slab B: User-managed cold data - allocate same number of slots */
    slab->slab_b.slot_size = align_up(config->aux_data_size, 8);
    if (slab->slab_b.slot_size == 0) {
        slab->slab_b.slot_size = 8; /* Minimum size */
    }
    slab->slab_b.slots_per_block = config->total_slots;
    slab->slab_b.block_capacity = 1;
    slab->slab_b.block_count = 0;
    slab->slab_b.free_list_idx = OM_SLOT_IDX_NULL;

    /* Allocate one block for all aux slots */
    size_t block_size = slab->slab_b.slot_size * config->total_slots;
    uint8_t *block = calloc(1, block_size);
    if (!block) {
        free(slab->slab_a.memory);
        return OM_ERR_SLAB_AUX_ALLOC;
    }
    
    slab->slab_b.blocks = calloc(1, sizeof(uint8_t *));
    if (!slab->slab_b.blocks) {
        free(block);
        free(slab->slab_a.memory);
        return OM_ERR_SLAB_AUX_ALLOC;
    }
    
    slab->slab_b.blocks[0] = block;
    slab->slab_b.block_count = 1;
    
    /* Build free list for aux slots */
    for (uint32_t i = 0; i < config->total_slots; i++) {
        uint32_t *next_ptr = (uint32_t *)(block + i * slab->slab_b.slot_size);
        *next_ptr = (i == 0) ? OM_SLOT_IDX_NULL : (i - 1);
    }
    slab->slab_b.free_list_idx = config->total_slots - 1;
    
    /* Initialize order ID counter */
    slab->next_order_id = 1;

    return 0;
}

void om_slab_destroy(OmDualSlab *slab) {
    free(slab->slab_a.memory);

    for (size_t i = 0; i < slab->slab_b.block_count; i++) {
        free(slab->slab_b.blocks[i]);
    }
    free(slab->slab_b.blocks);

    memset(slab, 0, sizeof(OmDualSlab));
}

OmSlabSlot *om_slab_alloc(OmDualSlab *slab) {
    /* Check if we have free slots in fixed slab */
    if (slab->slab_a.free_list_idx == OM_SLOT_IDX_NULL) {
        return NULL;
    }
    
    /* Get index from fixed slab free list */
    uint32_t idx = slab->slab_a.free_list_idx;
    OmSlabSlot *slot = idx_to_slot_a(&slab->slab_a, idx);
    if (!slot) return NULL;
    
    /* Update fixed slab free list */
    slab->slab_a.free_list_idx = slot->queue_nodes[OM_Q0_INTERNAL_FREE].next_idx;
    
    /* Check and update aux slab free list */
    if (slab->slab_b.free_list_idx == OM_SLOT_IDX_NULL) {
        /* No aux slot available, put fixed slot back */
        slot->queue_nodes[OM_Q0_INTERNAL_FREE].next_idx = slab->slab_a.free_list_idx;
        slab->slab_a.free_list_idx = idx;
        return NULL;
    }
    
    /* Verify we got matching index (should always match with proper init) */
    if (slab->slab_b.free_list_idx != idx) {
        /* This shouldn't happen if both slabs are in sync */
        /* Put fixed slot back */
        slot->queue_nodes[OM_Q0_INTERNAL_FREE].next_idx = slab->slab_a.free_list_idx;
        slab->slab_a.free_list_idx = idx;
        return NULL;
    }
    
    /* Update aux slab free list */
    uint8_t *aux_block = slab->slab_b.blocks[0];
    uint32_t *aux_next = (uint32_t *)(aux_block + idx * slab->slab_b.slot_size);
    slab->slab_b.free_list_idx = *aux_next;
    
    /* Clear both slots */
    slot->price = 0;
    slot->volume = 0;
    slot->volume_remain = 0;
    slot->org = 0;
    slot->flags = 0;
    slot->order_id = OM_SLOT_IDX_NULL;
    for (int q = 0; q < OM_MAX_QUEUES; q++) {
        slot->queue_nodes[q].next_idx = OM_SLOT_IDX_NULL;
        slot->queue_nodes[q].prev_idx = OM_SLOT_IDX_NULL;
    }
    
    /* Clear aux slot */
    memset(aux_block + idx * slab->slab_b.slot_size, 0, slab->slab_b.slot_size);
    
    slab->slab_a.used++;
    return slot;
}

void om_slab_free(OmDualSlab *slab, OmSlabSlot *slot) {
    uint32_t slot_idx = slot_to_idx_a(&slab->slab_a, slot);
    if (slot_idx == OM_SLOT_IDX_NULL) return;
    
    /* Clear fixed slot and add to free list */
    for (int q = 0; q < OM_MAX_QUEUES; q++) {
        slot->queue_nodes[q].next_idx = OM_SLOT_IDX_NULL;
        slot->queue_nodes[q].prev_idx = OM_SLOT_IDX_NULL;
    }
    slot->queue_nodes[OM_Q0_INTERNAL_FREE].next_idx = slab->slab_a.free_list_idx;
    slab->slab_a.free_list_idx = slot_idx;
    slab->slab_a.used--;
    
    /* Clear aux slot and add to free list */
    uint8_t *aux_block = slab->slab_b.blocks[0];
    uint32_t *aux_next = (uint32_t *)(aux_block + slot_idx * slab->slab_b.slot_size);
    *aux_next = slab->slab_b.free_list_idx;
    slab->slab_b.free_list_idx = slot_idx;
}

/* Generate next unique order ID (auto-increment, starts at 1) */
uint32_t om_slab_next_order_id(OmDualSlab *slab) {
    if (!slab->next_order_id) return 0;
    return slab->next_order_id++;
}
