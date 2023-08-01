options(warn = -1)
library(sparklyr)

default_config <- sparklyr::spark_config()

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "sampling",
    config = default_config)

config <- yaml::yaml.load_file("ons-spark/config.yaml")

rescue <- sparklyr::spark_read_csv(sc, config$rescue_path_csv, header=TRUE, infer_schema=TRUE)

skewed_df <- sparklyr::sdf_seq(sc,from = 1, to = 1e6) %>%
          sparklyr::mutate(skew_col = case_when(
          id <= 100 ~ 'A',
          id <= 1000 ~ 'B',
          id <= 10000 ~ 'C',
          id <= 100000 ~ 'D',
          .default = 'E'))


rescue_sample <- rescue %>% sparklyr::sdf_sample(fraction=0.1, replacement=FALSE)
rescue_sample %>% sparklyr::sdf_nrow()

print(paste0("Total rows in original DF: ", sparklyr::sdf_nrow(rescue)))
print(paste0("Total rows in sampled DF: ", sparklyr::sdf_nrow(rescue_sample)))
print(paste0("Fraction of rows sampled: ", sparklyr::sdf_nrow(rescue_sample)/sparklyr::sdf_nrow(rescue)))



rescue_sample_seed_1 <- rescue %>% sparklyr::sdf_sample(fraction=0.1, seed=99)
rescue_sample_seed_2 <- rescue %>% sparklyr::sdf_sample(fraction=0.1, seed=99)

print(paste0("Seed 1 count: ", rescue_sample_seed_1 %>% sparklyr::sdf_nrow()))
print(paste0("Seed 2 count: ", rescue_sample_seed_2 %>% sparklyr::sdf_nrow()))

replacement_sample = rescue %>% sparklyr::sdf_sample(fraction=0.1, replacement=TRUE, seed = 20)
replacement_sample %>% sparklyr::sdf_nrow()

replacement_sample %>%
    dplyr::group_by(IncidentNumber) %>%
    dplyr::count(IncidentNumber)

fraction <- 0.1
row_count <- round(sparklyr::sdf_nrow(rescue) * fraction)
row_count


rescue %>%
    sparklyr::mutate(rand_no = rand()) %>%
    dplyr::arrange(rand_no) %>%
    head(row_count) %>%
    sparklyr::select(rand_no) %>%
    sparklyr::sdf_nrow()


rescue %>%
    sparklyr::filter(CalYear == 2012 | CalYear == 2017) %>%
    sparklyr::sdf_nrow()

splits <- rescue %>% sparklyr::sdf_random_split(
    split1 = 0.5,
    split2 = 0.4,
    split3 = 0.1)

print(paste0("Split1: ", sparklyr::sdf_nrow(splits$split1)))
print(paste0("Split2: ", sparklyr::sdf_nrow(splits$split2)))
print(paste0("Split3: ", sparklyr::sdf_nrow(splits$split3)))

print(paste0("DF count: ", sparklyr::sdf_nrow(rescue)))
print(paste0("Split count total: ",
             sparklyr::sdf_nrow(splits$split1) +
             sparklyr::sdf_nrow(splits$split2) +
             sparklyr::sdf_nrow(splits$split3)))
