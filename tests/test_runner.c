#include <check.h>
#include <stdlib.h>

Suite* slab_suite(void);
Suite* orderbook_suite(void);
Suite* wal_suite(void);
Suite* engine_suite(void);
Suite* market_suite(void);
Suite* bus_suite(void);
Suite* bus_mp_suite(void);

int main(void) {
    int number_failed;
    SRunner *sr = srunner_create(slab_suite());
    srunner_add_suite(sr, orderbook_suite());
    srunner_add_suite(sr, wal_suite());
    srunner_add_suite(sr, engine_suite());
    srunner_add_suite(sr, market_suite());
    srunner_add_suite(sr, bus_suite());
    srunner_add_suite(sr, bus_mp_suite());

    srunner_run_all(sr, CK_NORMAL);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);
    
    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
