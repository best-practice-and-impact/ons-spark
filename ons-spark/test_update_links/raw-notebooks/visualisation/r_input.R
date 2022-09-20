options(warn = -1)
library(sparklyr)
library(dplyr)
library(ggplot2)

default_config <- sparklyr::spark_config()

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "data-viz",
    config = default_config)

config <- yaml::yaml.load_file("ons-spark/config.yaml")

rescue <- sparklyr::spark_read_parquet(sc, config$rescue_path) %>%
    dplyr::group_by(cal_year) %>%
    dplyr::summarise(count = n()) %>%
    dplyr::arrange(cal_year)

rescue_tb <- rescue %>% sparklyr::collect()

ggplot2::ggplot(data=rescue_tb, ggplot2::aes(x=as.factor(cal_year), y=count)) +
    ggplot2::geom_bar(stat="identity") +
    ggplot2::labs(x="cal_year")

ggplot2::ggplot(data=rescue, ggplot2::aes(x=as.factor(cal_year), y=count)) +
    ggplot2::geom_bar(stat="identity") +
    ggplot2::labs(x="cal_year")
