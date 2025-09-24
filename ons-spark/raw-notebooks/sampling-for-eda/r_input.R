

# Load in required packages
library(sparklyr)
library(dplyr)
library(pillar)

# Set up Spark session
default_config <- sparklyr::spark_config()

sc <- spark_connect(master = "local",
                  app_name = "sampling_for_eda",
                  config = default_config)


# Check Spark connection is open
  spark_connection_is_open(sc)




# Read in the MOT dataset
#mot_path = "s3a://onscdp-dev-data01-5320d6ca/bat/dapcats/mot_test_results.csv"
mot_path = "C:/Users/snowda/repos/data/mot_test_results.csv"
mot <- sparklyr::spark_read_csv(
    sc,
    mot_path,
    header = TRUE,
    infer_schema = TRUE)

# Check schema and preview data
pillar::glimpse(mot)




# Check the size of the data we are working with
mot %>%
    sparklyr::sdf_nrow() %>%
    print()




# Select columns we want to work with
mot <- mot %>%
  sparklyr::select(vehicle_id, test_date, test_mileage, postcode_area, make, colour, cylinder_capacity)

# Change column data type or column name formatting. For example, change test_date to a date instead of an integer.
mot %>% 
    dplyr::mutate(test_date = as.date(test_date))

# Re-check the schema to ensure your changes have been made to the mot dataframe
pillar::glimpse(mot)




# Check for missing data first, do not just omit it. The example uses the test_mileage column.

mot %>% 
    filter(is.na(test_mileage)) %>% 
    sdf_nrow() %>% 
    print()




# If appropriate for your data, remove any rows with missing data under ANY variable.
mot <- mot %>%
    na.omit()

 mot %>% sdf_nrow()



# Identify duplicate data in the mot dataframe
duplicates <- mot %>%
  group_by(vehicle_id, test_date, test_mileage, postcode_area, make, colour, cylinder_capacity) %>%
  summarise(count = n()) %>%
  filter(count > 1)
  arrange(desc(count))

duplicates %>%
    sdf_nrow() %>%
    print()




# If appropriate for your data, remove the duplicated rows and preview your clean dataset 
(i.e. removal of duplicates and missing values).

mot_clean <- sdf_distinct(mot)

mot_clean_size <- mot_clean %>%
                            sparklyr::sdf_nrow()

mot_clean_size %>% print()

pillar::glimpse(mot_clean)




# It can also be useful to view distinct groups in the categorical columns across your data frame, for example showing the distinct groups in the 'colour' column will show you all the different colours of cars reported in the dataframe. This could also help you spot anomalies or errors.
colour <- mot_clean %>% 
  sparklyr::select(colour) %>% 
  sparklyr::sdf_distinct() %>%
  sparklyr::sdf_collect()

 colour 




colour
# A tibble: 20 × 1
   colour
   <chr>
 1 RED
 2 CREAM
 3 GOLD
 4 BROWN
 5 YELLOW
 6 BRONZE      
 7 BLACK
 8 MULTI-COLOUR
 9 NOT STATED
10 SILVER
11 PINK
12 PURPLE
13 BLUE
14 GREY
15 BEIGE
16 MAROON
17 TURQUOISE
18 WHITE
19 ORANGE
20 GREEN




corr_matrix <- mot_distinct %>%
              ml_corr(c("vehicle_id", "test_mileage", "cylinder_capacity"))
corr_matrix




# Summary statistics for a numeric and string column 
summary_mileage <- sdf_describe(mot_clean, "test_mileage")
summary_mileage



sample_size_finite <- function(N, z, p, e) {
    #' Calculates a suggested sample size for a finite population.
    #'
    #' @param N = population size
    #' @param z = z score (e.g. 1.96 = 95% confidence)
    #' @param p = population proportion (use 0.5 if unknown)
    #' @param e = margin of error (e.g. 0.05 for 5%)

  # Initial sample size without finite population correction
  n <- (z^2 * p * (1 - p)) / (e^2)
  
  # Adjusted sample size for finite population
  adjusted_size <- n / (1 + (n / N))
  
  # Return the ceiling of the adjusted sample size. 
  print(ceiling(adjusted_size))
}

# We will now use the function to determine a sample size for the mot_clean data, based on a 99.99% confidence interval and 1% margin of error.
# As we set the mot_clean_size variable we will input this as N.

sample_size <- sample_size_finite(mot_clean_size, 3.89, 0.5, 0.01)



fraction <- sample_size/mot_clean_size %>% print()
mot_sample <- mot_clean %>% sparklyr::sdf_sample(fraction, replacement=FALSE, seed = 99)
mot_sample %>% sparklyr::sdf_nrow()





# Read in the sample data ready for EDA
mot_eda_sample <- read.csv("D:/repos/ons-spark/ons-spark/data/mot_eda_sample_0.1.csv")

# Check schema and preview data
pillar::glimpse(mot_eda_sample)




# Again, check the data types of each column in the dataframe, change them if necessary

mot_eda_sample <- mot_eda_sample %>% 
    dplyr::mutate(test_date = as.Date(test_date)) %>%
    dplyr::mutate_at(c("make", "colour"), as.factor)




summary <- summary(mot_eda_sample)
summary




colour_percentage <- mot_eda_sample %>% 
              dplyr::group_by(colour) %>% 
              dplyr::summarise(count = length(colour)) %>%
              dplyr::mutate(percentage = count/(nrow(mot_eda_sample))*100) %>%
              arrange(desc(percentage))




mean_mileage <- mot_eda_sample %>% 
                group_by(colour) %>% 
                summarise(mean_mileage = mean(test_mileage, na.rm = TRUE)) %>%
                arrange(desc(mean_mileage)) %>%
                print()




library(ggplot2)

ggplot(mean_mileage, aes(x = reorder(colour, -mean_mileage), y = mean_mileage)) +
  geom_bar(stat = "identity", fill = "skyblue") +
  geom_text(aes(label = round(mean_mileage, 0)), vjust = 1, angle = 45) +
  labs(
    title = "Mean Mileage by Car Colour",
    x = "Car Colour",
    y = "Mean Mileage"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

