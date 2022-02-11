options(warn = -1)
library(sparklyr)

default_config <- sparklyr::spark_config()

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "spark-app-ui",
    config = default_config)

library("cdsw")
url = paste0("spark-", Sys.getenv("CDSW_ENGINE_ID"), ".", Sys.getenv("CDSW_DOMAIN"), sep="")
html(paste0("<a href=http://", url, ">Spark UI</a>", sep=""))

config <- yaml::yaml.load_file("ons-spark/config.yaml")

rescue <- sparklyr::spark_read_parquet(sc, config$rescue_path)

print(paste0("Number of partitions: ", sparklyr::sdf_num_partitions(rescue)))

rescue <- rescue %>% dplyr::mutate(
    cost_group=dplyr::case_when(total_cost<300 ~ "small",
                                total_cost>=300 & total_cost<900 ~ "medium",
                                total_cost>=900 ~ "large",
                                TRUE ~ NULL)
)


rescue %>% 
    dplyr::select("animal_group", "description", "total_cost", "cost_group") %>%
    head(7)

rescue %>% sparklyr::sdf_nrow()

aggregated_spark <- rescue %>% 
    dplyr::group_by(cost_group) %>%
    dplyr::summarise(count=n())

aggregated_r <- aggregated_spark %>% sparklyr::collect()

aggregated_r

aggregated_r_plot <- aggregated_r %>% dplyr::arrange(desc(cost_group))

ggplot2::ggplot(aggregated_r_plot, ggplot2::aes(cost_group, count)) +
    ggplot2::geom_bar(stat="identity")

aggregated_spark <- rescue %>% 
    dplyr::group_by(cost_group) %>%
    dplyr::summarise(count=n()) %>%
    sparklyr::sdf_coalesce(2)

                                    
aggregated_r <- aggregated_spark %>% 
    sparklyr::collect()
