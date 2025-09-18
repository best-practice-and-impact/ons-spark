

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

