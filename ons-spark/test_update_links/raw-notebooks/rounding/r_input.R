options(warn = -1)
library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "rounding",
    config = sparklyr::spark_config())

sdf <- sparklyr:::sdf_seq(sc, -7, 8, 2) %>%
    sparklyr::mutate(half_id = id / 2) %>%
    sparklyr::select(half_id)
    
sdf %>%
    sparklyr::collect() %>%
    print()

sdf <- sdf %>%
    sparklyr::mutate(spark_round = round(half_id))

sdf %>%
    sparklyr::collect() %>%
    print()

tdf <- sdf %>%
    sparklyr::collect() %>%
    sparklyr::mutate(r_round = round(half_id)) %>%
    print()

sdf <- sdf %>%
    sparklyr::mutate(spark_bround = bround(half_id))

sdf %>%
    sparklyr::collect() %>%
    print()
