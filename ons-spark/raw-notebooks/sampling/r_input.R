options(warn = -1)
library(sparklyr)

default_config <- sparklyr::spark_config()

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "sampling",
    config = default_config)

config <- yaml::yaml.load_file("ons-spark/config.yaml")

rescue <- sparklyr::spark_read_csv(sc, config$rescue_path_csv, header=TRUE, infer_schema=TRUE)
rescue <- rescue %>% sparklyr::mutate(animal_type = AnimalGroupParent)
rescue <- select(rescue,-AnimalGroupParent)

skewed_df <- sparklyr::sdf_seq(sc,from = 1, to = 1e6) %>%
          sparklyr::mutate(skew_col = case_when(
          id <= 100 ~ 'A',
          id <= 1000 ~ 'B',
          id <= 10000 ~ 'C',
          id <= 100000 ~ 'D',
          .default = 'E'))

skewed_df <- skewed_df %>% sparklyr::sdf_repartition(partition_by = 'skew_col')


rescue_sample <- rescue %>% sparklyr::sdf_sample(fraction=0.1, replacement=FALSE)
rescue_sample %>% sparklyr::sdf_nrow()

print(paste0("Total rows in original DF: ", sparklyr::sdf_nrow(rescue)))
print(paste0("Total rows in sampled DF: ", sparklyr::sdf_nrow(rescue_sample)))
print(paste0("Fraction of rows sampled: ", sparklyr::sdf_nrow(rescue_sample)/sparklyr::sdf_nrow(rescue)))



rescue_sample_seed_1 <- rescue %>% sparklyr::sdf_sample(fraction=0.1, seed=99)
rescue_sample_seed_2 <- rescue %>% sparklyr::sdf_sample(fraction=0.1, seed=99)

print(paste0("Seed 1 count: ", rescue_sample_seed_1 %>% sparklyr::sdf_nrow()))
print(paste0("Seed 2 count: ", rescue_sample_seed_2 %>% sparklyr::sdf_nrow()))

n_rows <- skewed_df %>% sparklyr::sdf_nrow()
skewed_df %>%
        dplyr::group_by(skew_col) %>%
        dplyr::count(skew_col,name = 'row_count') %>%
        sparklyr::mutate(percentage_of_dataframe = row_count/n_rows*100) %>%
        sdf_sort('skew_col')

skewed_sample <- skewed_df %>% sparklyr::sdf_sample(fraction= 0.1, replacement= FALSE)

n_rows_sample <- skewed_sample %>% sparklyr::sdf_nrow()

skewed_sample %>%
        dplyr::group_by(skew_col) %>%
        dplyr::count(skew_col,name = 'row_count') %>%
        sparklyr::mutate(percentage_of_dataframe = row_count/n_rows_sample*100) %>%
        sdf_sort('skew_col')

equal_partitions_df  <- skewed_df %>% sparklyr::sdf_repartition(20)

equal_partitions_sample <- equal_partitions_df %>% sparklyr::sdf_sample(fraction= 0.1, replacement= FALSE)

n_rows_sample_equal <- equal_partitions_sample %>% sparklyr::sdf_nrow()

equal_partitions_sample %>%
        dplyr::group_by(skew_col) %>%
        dplyr::count(skew_col,name = 'row_count') %>%
        sparklyr::mutate(percentage_of_dataframe = row_count/n_rows_sample_equal*100) %>%
        sdf_sort('skew_col')

replacement_sample = rescue %>% sparklyr::sdf_sample(fraction=0.1, replacement=TRUE, seed = 20)
replacement_sample %>% sparklyr::sdf_nrow()

replacement_sample %>%
    dplyr::group_by(IncidentNumber) %>%
    dplyr::count(IncidentNumber)%>%
    dplyr::arrange(desc(n))

fraction <- 0.1
row_count <- round(sparklyr::sdf_nrow(rescue) * fraction)
row_count


rescue %>%
    sparklyr::mutate(rand_no = rand()) %>%
    dplyr::arrange(rand_no) %>%
    head(row_count) %>%
    sparklyr::select(rand_no) %>%
    sparklyr::sdf_nrow()


# Preprocessing to simplify the number of animals in the `animal_type` column
simplified_animal_types <-rescue %>% sparklyr::mutate(animal_type = case_when(!animal_type %in% c('Cat','Bird','Dog','Fox') ~ 'Other',
                                                       .default = animal_type))

# Counting the number of animals in each group
simplified_animal_types %>%
        dplyr::group_by(animal_type) %>%
        dplyr::count(animal_type,name = 'row_count') %>%
        sdf_sort('animal_type')

simplified_animal_types <- simplified_animal_types %>% 
                            sparklyr::mutate(sample_size = case_when(
                                            animal_type == 'Bird' ~ 15,
                                            animal_type == 'Cat' ~ 20,
                                            animal_type == 'Dog' ~ 10,
                                            animal_type == 'Fox' ~ 2,
                                            animal_type == 'Other' ~ 5,
                                            .default = 0)) %>%
                            sparklyr::mutate(random_number = rand())


simplified_animal_types <- simplified_animal_types %>% 
              dplyr::group_by(animal_type) %>%
              sparklyr::mutate(strata_rank = rank(desc(random_number))) %>%
              dplyr::ungroup()

simplified_animal_types <- simplified_animal_types %>% sparklyr::mutate(sampled = case_when(
              strata_rank <= sample_size ~ 1,
              .default = 0))

sampled <- simplified_animal_types %>% sparklyr::filter(sampled == 1)

sampled %>%
        dplyr::group_by(animal_type,sample_size) %>%
        dplyr::count(animal_type,name = 'row_count') %>%
        sdf_sort('animal_type')


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

rescue_id <- rescue %>% sparklyr::sdf_repartition(20)

rescue_id <- rescue_id %>%
                  sparklyr::sdf_with_unique_id(id = "id") %>%
                  sparklyr::mutate(group_number = id%%3)

rescue_subsample_1 <- rescue_id %>% filter(group_number == 0)
rescue_subsample_2 <- rescue_id %>% filter(group_number == 1)
rescue_subsample_3 <- rescue_id %>% filter(group_number == 2)

cat(rescue_subsample_1 %>% sparklyr::sdf_nrow(),
rescue_subsample_2 %>% sparklyr::sdf_nrow(),
rescue_subsample_3 %>% sparklyr::sdf_nrow())
