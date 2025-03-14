
library(sparklyr)
library(data.table)
library(dplyr)
library(readr)
library(janitor)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "window-functions",
    config = sparklyr::spark_config())

config <- yaml::yaml.load_file("ons-spark/config.yaml")



uniform_dist <- runif(n = 1000, # number of observations/rows
                      min = 0,  # lower limit
                      max = 1)  # upper limit

hist(uniform_dist, main="Uniform Distribution Example", xlab="Value")

summary(uniform_dist)