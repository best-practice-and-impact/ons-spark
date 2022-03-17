options(warn = -1)
library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "cache",
    config = sparklyr::spark_config())

config <- yaml::yaml.load_file("ons-spark/config.yaml")

population <- sparklyr::spark_read_parquet(
    sc,
    path=config$population_path,
    name="population")

sparklyr::tbl_cache(sc, "population", force=TRUE)

sparklyr::tbl_uncache(sc, "population")

sparklyr::sdf_persist(population, storage.level = "DISK_ONLY", name = "population") %>%
    sparklyr::sdf_nrow()

sparklyr::tbl_uncache(sc, "population")

rescue <- sparklyr::spark_read_csv(sc, config$rescue_path_csv, header=TRUE, infer_schema=TRUE) 

rescue <- rescue %>% sparklyr::sdf_repartition(2)

rescue <- rescue %>% sparklyr::select(
     - WardCode, 
     - BoroughCode, 
     - Easting_m, 
     - Northing_m, 
     - Easting_rounded, 
     - Northing_rounded
)


rescue <- rescue %>% dplyr::rename(
    EngineCount = PumpCount,
    Description = FinalDescription,
    HourlyCost = HourlyNotionalCostGBP,
    TotalCost = IncidentNotionalCostGBP,
    OriginOfCall = OriginofCall,
    JobHours = PumpHoursTotal,
    AnimalGroup = AnimalGroupParent
)


rescue <- rescue %>% sparklyr::mutate(
    DateTimeOfCall = to_date(DateTimeOfCall, "dd/MM/yyyy")) 



rescue <- rescue %>% 
    sparklyr::mutate(IncidentDuration = JobHours / EngineCount)


rescue <- rescue  %>% 
      sparklyr::filter(!is.na(TotalCost) | !is.na(IncidentDuration))

get_top_10_incidents <- function(sdf) {
        sdf %>% 
            sparklyr::select(CalYear, PostcodeDistrict, AnimalGroup, IncidentDuration, TotalCost) %>%
            dplyr::arrange(dplyr::desc(TotalCost)) %>%
            head(10)
}

get_mean_cost_by_animal <- function(sdf) {
    sdf %>%
        dplyr::group_by(AnimalGroup) %>%
        dplyr::summarise(MeanCost = mean(TotalCost)) %>%
        dplyr::arrange(desc(MeanCost)) %>%
        head(10)
}

get_summary_cost_by_animal <- function(sdf) {
    sdf %>%
        sparklyr::filter(AnimalGroup %in% c(
            "Goat", 
            "Bull", 
            "Fish", 
            "Horse")) %>%
        dplyr::group_by(AnimalGroup) %>%
        dplyr::summarise(
            Min = min(TotalCost),
            Mean = avg(TotalCost),
            Max = max(TotalCost),
            Count = n()) %>%
        dplyr::arrange(desc(Mean))
    }

top_10_incidents <- get_top_10_incidents(rescue)

top_10_incidents %>%
    sparklyr::collect() %>%
    print()

mean_cost_by_animal <- get_mean_cost_by_animal(rescue)
mean_cost_by_animal %>%
    sparklyr::collect() %>%
    print()

summary_cost_by_animal <- get_summary_cost_by_animal(rescue)
summary_cost_by_animal %>%
    sparklyr::collect() %>%
    print()

rescue <- sparklyr::sdf_register(rescue, "rescue")
sparklyr::tbl_cache(sc, "rescue", force=TRUE)

get_top_10_incidents(rescue) %>%
    sparklyr::collect() %>%
    print()

get_mean_cost_by_animal(rescue) %>%
    sparklyr::collect() %>%
    print()

get_summary_cost_by_animal(rescue) %>%
    sparklyr::collect() %>%
    print()
