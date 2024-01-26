options(warn = -1)
library(sparklyr)
library(dplyr)

default_config <- sparklyr::spark_config()

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "flags",
    config = default_config)


set.seed(42)

df = sparklyr::sdf_seq(sc, 0, 4) %>%
    rename("group" = "id") %>%
    cross_join(sparklyr::sdf_seq(sc, 0, 2)) %>%
    rename("drop" = "id") %>%
    select(-c(drop)) %>%
    mutate(age = ceil(rand()*10))
       
print(df,n=15)

df <- df %>%
    group_by(group) %>%
    arrange(desc(age)) %>%
    mutate(age_lag = lag(age,n = -1))  %>%
    mutate(age_diff = age - age_lag) %>%
    group_by(group) %>%
    mutate(age_order = row_number()) %>%
    mutate(top_two_age_diff = ifelse(age_order == 1,
                                  age_diff,
                                  0
                                  )) %>%
    mutate(age_diff_flag = ifelse(top_two_age_diff > 5,
                                  1,
                                  0
                                  ))

print(df, n = 15)
