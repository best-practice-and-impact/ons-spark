
# Load the R packages
rm(list = ls()) 
library(dplyr)
library(readr)
library(janitor)
library(magrittr)

set.seed(12345)

uniform_dist <- runif(n = 1000, 
                      min = 0,  
                      max = 1)
hist(uniform_dist, main="Uniform Distribution Example", xlab="Value")
summary(uniform_dist)

normal_dist <- rnorm(n = 1000,
                     mean = 50,
                     sd = 4)
hist(normal_dist, main="Normal Distribution Example", xlab="Value")
summary(normal_dist)

poisson_dist <- rpois(n = 1000,  
                      lambda = 4)  

hist(poisson_dist, main="Poisson Distribution Example", xlab="Number of Events")
summary(poisson_dist)

binomial_dist <- rbinom(n = 1000,   
                        size = 20,  
                        prob = 0.2)  

hist(binomial_dist, main="Binomial Distribution Example", xlab="Number of Successes")
summary(binomial_dist)

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

library(data.table)

synthetic_data1 <- data.table(uniform_dist,
                              normal_dist,
                              poisson_dist,
                              binomial_dist,
                              gender,
                              marriage_status)

print(head(synthetic_data1))
summary(synthetic_data1)
str(synthetic_data1)

# Change gender and marriage_status to factor variables
synthetic_data1$gender <- as.factor(synthetic_data1$gender)
synthetic_data1$marriage_status <- as.factor(synthetic_data1$marriage_status)

# Check the structure of the dataset again
str(synthetic_data1)

install.packages("synthpop")

# Clean out workspace
rm(list = ls())

library(synthpop)
library(dplyr)
library(readr)
library(janitor)
library(magrittr)
library(data.table)

# This will give you the dimensions (rows, columns) of the SD2011 dataset
dim(SD2011)  # Output: 5000 rows, 35 variables

# Show summary information for a subset of variables; here we show only the first five variables for brevity.
codebook.syn(SD2011[, 1:5])$tab

# Select a smaller subset of variables for demonstration
mydata <- SD2011[, c(1, 2, 3, 6, 8, 10, 11)]
# Preview the first few rows of the dataset
head(mydata)
# Get a summary of the dataset
summary(mydata)

# Check for negative income values
table(mydata$income[mydata$income < 0], useNA = "ifany")

set.seed(123)

# Synthesise data, handling -8 as a missing value for income
mysyn <- syn(mydata, cont.na = list(income = -8))

summary(mysyn)

# Compare the synthetic data with the original data
compare(mysyn, mydata, stat = "counts")

# Export synthetic data to CSV format
write.syn(mysyn, filename = "mysyn_SD2001", filetype = "csv")

names(mysyn)

mysyn$method
mysyn$predictor.matrix
mysyn$visit.sequence
mysyn$cont.na
mysyn$seed

# Additional comparisons
multi.compare(mysyn, mydata, var = "marital", by = "sex")
multi.compare(mysyn, mydata, var = "income", by = "agegr")
multi.compare(mysyn, mydata, var = "income", by = "edu", cont.type = "boxplot")

# Load the configuration and set the path to the census teaching data
# config <- yaml::yaml.load_file("/home/cdsw/ons-spark/config.yaml")
config <- yaml::yaml.load_file("D:/dapcats_guidance/ons-spark/config.yaml")
census_2011_path = config$census_2011_teaching_data_path_csv
census_teaching_data <- data.table::fread(census_2011_path, skip = 1) %>%
                        janitor::clean_names()

# Create a subset of the data
small_census_teaching_data <- census_teaching_data[1:100000,]

# First synthesis
synthetic_census_teaching_data <- synthpop::syn(data = small_census_teaching_data,
                                                method = "sample",
                                                k = 100000)

# Check the class of the synthetic data object
class(synthetic_census_teaching_data)

summary(synthetic_census_teaching_data)

# Compare synthetic data with real data
synthpop::compare(object = synthetic_census_teaching_data,
                  data = small_census_teaching_data)

#config <- yaml::yaml.load_file("/home/cdsw/ons-spark/config.yaml")
config <- yaml::yaml.load_file("D:/dapcats_guidance/ons-spark/config.yaml")
census_relationship_path = config$census_relationship_file_path_csv

input <- read.csv(census_relationship_path,
                  stringsAsFactors = FALSE,
                  skip = 1) %>%
         dplyr::select(variable, description, predictors)

# Add row names to make it easier
rownames(input) <- input$variable

# Create predictor matrix in appropriate format for synthpop from CSV input
predictor_matrix <- as.data.frame(matrix(0, nrow = nrow(input), 
                                         ncol = nrow(input), 
                                         dimnames = list(input$variable, input$variable)))

# Use predictors
for (var in input$variable) {
  predictor_matrix[var, as.vector((strsplit(input[var, 'predictors'], " "))[[1]])] <- 1
}

# Relationships can be added manually (row variable predicted by column variable)
predictor_matrix['Marital_Status', 'Age'] <- 1
predictor_matrix['Economic_Activity', 'Age'] <- 1
predictor_matrix['Occupation', 'Economic_Activity'] <- 1
predictor_matrix['Hours_worked_per_week', 'Economic_Activity'] <- 1

# Synthesise data with relationships
synthetic_census_teaching_data_2 <- synthpop::syn(data = small_census_teaching_data,
                                                  predictor.matrix = as.matrix(predictor_matrix),
                                                  minnumlevels = 10)

# Compare synthetic data with real data
synthpop::compare(data = small_census_teaching_data,
                  object = synthetic_census_teaching_data_2)

# Define variables to compare
compare_vars <- c("economic_activity", "hours_worked_per_week")

# Frequency table of compare_vars in original data
orig_table <- small_census_teaching_data[, .(orig_count = .N), keyby = compare_vars]

# Convert to factors
orig_table <- orig_table %>% 
              dplyr::mutate(economic_activity = as.factor(economic_activity),
                            hours_worked_per_week = as.factor(hours_worked_per_week))

orig_table

# Frequency table of compare_vars in synthetic data with relationships
synth2_table <- as.data.table(synthetic_census_teaching_data_2$syn)[, .(synth2_count = .N), keyby = compare_vars]

synth2_table

# Convert economic_activity in synth2_table to a factor
synth2_table <- synth2_table %>% 
                dplyr::mutate(economic_activity = as.factor(economic_activity),
                              hours_worked_per_week = as.factor(hours_worked_per_week))

# Frequency table of compare_vars in basic synthetic data
synth1_table <- as.data.table(synthetic_census_teaching_data$syn)[, .(synth1_count = .N), keyby = compare_vars]

synth1_table

# Convert to factors
synth1_table <- synth1_table %>% 
              dplyr::mutate(economic_activity = as.factor(economic_activity),
                            hours_worked_per_week = as.factor(hours_worked_per_week))


# Combine counts from both datasets for comparisons
comparison2_table <- merge(orig_table, synth2_table, all = TRUE)
comparison1_table <- merge(orig_table, synth1_table, all = TRUE)

# Replace NA with 0
comparison1_table <- comparison1_table %>% 
                     dplyr::mutate(orig_count = as.double(orig_count)) %>% 
                     dplyr::mutate(orig_count = if_else(condition = is.na(orig_count),
                                                        true = 0, 
                                                        false = orig_count))


# NAs occur if the combination is not present in one of the datasets, convert these to zero counts
comparison2_table[is.na(orig_count), orig_count := 0]
comparison2_table[is.na(synth2_count), synth2_count := 0]
comparison1_table[is.na(orig_count), orig_count := 0]
comparison1_table[is.na(synth1_count), synth1_count := 0]

# Average absolute distance
comparison2_table[, mean(abs(orig_count - synth2_count))]
comparison1_table[, mean(abs(orig_count - synth1_count))]

# Average percentage difference
comparison2_table[, 100 * mean((abs(orig_count - synth2_count) / orig_count))]
comparison1_table[, 100 * mean((abs(orig_count - synth1_count) / orig_count))]
