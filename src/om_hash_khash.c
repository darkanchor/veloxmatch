#include <stdlib.h>
#include <string.h>
#include "khash.h"
#include "../include/openmatch/om_hash.h"

OmHashMap *om_hash_create(size_t initial_capacity) {
    OmHashMap *map = calloc(1, sizeof(OmHashMap));
    if (!map) return NULL;

    map->hash = kh_init(entry);
    if (!map->hash) {
        free(map);
        return NULL;
    }

    if (initial_capacity > 0) {
        if (kh_resize(entry, map->hash, initial_capacity) < 0) {
            kh_destroy(entry, map->hash); free(map); return NULL;
        }
    }

    return map;
}

void om_hash_destroy(OmHashMap *map) {
    if (!map) return;

    if (map->hash) {
        kh_destroy(entry, map->hash);
    }
    free(map);
}

bool om_hash_insert(OmHashMap *map, uint64_t key, OmOrderEntry value) {
    if (!map || !map->hash) return false;

    int ret;
    khiter_t k = kh_put(entry, map->hash, key, &ret);

    if (ret < 0) {
        return false;
    }

    kh_value(map->hash, k) = value;
    return true;
}

OmOrderEntry *om_hash_get(OmHashMap *map, uint64_t key) {
    if (!map || !map->hash) return NULL;

    khiter_t k = kh_get(entry, map->hash, key);
    if (k == kh_end(map->hash)) {
        return NULL;
    }

    return &kh_value(map->hash, k);
}

bool om_hash_remove(OmHashMap *map, uint64_t key) {
    if (!map || !map->hash) return false;

    khiter_t k = kh_get(entry, map->hash, key);
    if (k == kh_end(map->hash)) {
        return false;
    }

    kh_del(entry, map->hash, k);
    return true;
}

bool om_hash_contains(OmHashMap *map, uint64_t key) {
    if (!map || !map->hash) return false;

    khiter_t k = kh_get(entry, map->hash, key);
    return k != kh_end(map->hash);
}

size_t om_hash_size(const OmHashMap *map) {
    if (!map || !map->hash) return 0;

    return kh_size(map->hash);
}
