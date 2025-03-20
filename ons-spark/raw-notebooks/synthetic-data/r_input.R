
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



uniform_dist <- runif(n = 1000,
                      min = 0,
                      max = 1)

hist(uniform_dist, main = "Uniform Distribution Example", xlab = "Value")
summary(uniform_dist)

normal_dist <- rnorm(n = 1000,
                     mean = 50,
                     sd = 4)
hist(normal_dist, main = "Normal Distribution Example", xlab = "Value")
summary(normal_dist)

poisson_dist <- rpois(n = 1000,
                      lambda = 4)
hist(poisson_dist,
     main = "Poisson Distribution Example",
     xlab = "Number of Events")
summary(poisson_dist)

binomial_dist <- rbinom(n = 1000,
                        size = 20,
                        prob = 0.2)
hist(binomial_dist,
     main = "Binomial Distribution Example",
     xlab = "Number of Successes")
summary(binomial_dist)

# Set seed for reproducibility
set.seed(123)

# Random sampling from a vector
gender <- sample(x = c("M", "F"),
                 size = 1000,
                 replace = TRUE)
table(gender)
prop.table(table(gender))

marriage_status <- sample(x = c("Single", "Married", "Divorced", "Widowed"),
                          size = 1000,
                          replace = TRUE,
                          prob = c(0.35, 0.50, 0.10, 0.05))
table(marriage_status)
prop.table(table(marriage_status))

# Combine the data into a synthetic dataset
synthetic_data1 <- data.table(uniform_dist,
                              normal_dist,
                              poisson_dist,
                              binomial_dist,
                              gender,
                              marriage_status)

# Explore the synthetic dataset
print(head(synthetic_data1))
summary(synthetic_data1)
str(synthetic_data1)

# Change gender and marriage_status to factor variables
synthetic_data1$gender <- as.factor(synthetic_data1$gender)
synthetic_data1$marriage_status <- as.factor(synthetic_data1$marriage_status)

# Check the structure of the dataset again
str(synthetic_data1)