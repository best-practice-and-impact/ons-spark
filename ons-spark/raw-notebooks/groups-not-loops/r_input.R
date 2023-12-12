library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
  master = "local[2]",
  app_name = "ons-spark",
  config = sparklyr::spark_config(),
  )

config <- yaml::yaml.load_file("ons-spark/config.yaml")


install.packages("sparklyr", dependencies = TRUE, type = "source", INSTALL_opts = "--no-multiarch")
