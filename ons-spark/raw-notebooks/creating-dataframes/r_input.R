options(warn = -1)
library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "window-functions",
    config = sparklyr::spark_config())

seed_no <- 100L
random_numbers = sparklyr::sdf_seq(sc, 0, 4) %>%
    sparklyr::mutate(rand_no = rand(seed_no))

random_numbers %>%
    sparklyr::collect() %>%
    print()

winners_rdf <- data.frame(
    "year" = 2017:2021,
    "winner" = c("Minella Times", NA, "Tiger Roll", "Tiger Roll", "One For Arthur"),
    "starting_price" = c("11/1", NA, "4/1 F", "10/1", "14/1"),
    "age" = c(8, NA, 9, 8, 8),
    "jockey" = c("Rachael Blackmore", NA, "Davy Russell", "Davy Russell", "Derek Fox")
)

winners_rdf %>%
    print()

winners_spark <- sparklyr::sdf_copy_to(sc, winners_rdf)

winners_spark %>%
    sparklyr::collect() %>%
    print()
