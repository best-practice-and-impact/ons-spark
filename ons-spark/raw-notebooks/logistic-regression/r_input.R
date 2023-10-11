 
library(sparklyr)
library(dplyr)
library(broom)

# set up spark session
sc <- sparklyr::spark_connect(
  master = "local[2]",
  app_name = "logistic-regression",
  config = sparklyr::spark_config())

# check connection is open
sparklyr::spark_connection_is_open(sc)

# read in data
rescue_path_parquet = "/training/rescue_clean.parquet"
rescue <- sparklyr::spark_read_parquet(sc, rescue_path_parquet)

# preview data
dplyr::glimpse(rescue)


# Create is_cat column to contain target variable and select relevant predictors
rescue_cat <- rescue %>% 
  dplyr::mutate(is_cat = ifelse(animal_group == "Cat", 1, 0)) %>% 
  sparklyr::select(typeofincident, 
                   engine_count, 
                   job_hours, 
                   hourly_cost, 
                   total_cost, 
                   originofcall, 
                   propertycategory,
                   specialservicetypecategory,
                   incident_duration,
                   is_cat)

dplyr::glimpse(rescue_cat)

 
# Convert engine_count, job_hours, hourly_cost, total_cost and incident_duration columns to numeric
rescue_cat <- rescue_cat %>% 
dplyr::mutate(across(c(engine_count:total_cost, incident_duration), 
                    ~as.numeric(.)))

# Check data types are now correct
dplyr::glimpse(rescue_cat)

 
# Get the number of NAs in the dataset by column
rescue_cat %>%
  dplyr::summarise_all(~sum(as.integer(is.na(.)))) %>% 
  print(width = Inf)

# There are 38 missing values in 4 of the columns 
# We can see that these are all on the same rows by filtering for NAs in one of the columns:
rescue_cat %>%
  sparklyr::filter(is.na(total_cost)) %>%
  print(n=38)

# For simplicity, we will just filter out these rows:
rescue_cat <- rescue_cat %>%
  sparklyr::filter(!is.na(total_cost)) 

# Double check we have no NAs left: 
rescue_cat %>%
  dplyr::summarise_all(~sum(as.integer(is.na(.)))) %>% 
  print(width = Inf)

 
  
# Run the model
glm_out <- sparklyr::ml_logistic_regression(rescue_cat, 
                                            formula = "is_cat ~ engine_count + job_hours + hourly_cost + originofcall + propertycategory + specialservicetypecategory")

 
# Get model predictions
sparklyr::ml_predict(glm_out) %>% 
  print(n = 20, width = Inf)

 
# Run the model
glm_out <- sparklyr::ml_logistic_regression(rescue_cat, 
                                            formula = "is_cat ~ engine_count + job_hours + hourly_cost + originofcall + propertycategory + specialservicetypecategory")

# View coefficients
broom::tidy(glm_out)

 
# Run model with ml_generalized_linear_regression
glm_out <- sparklyr::ml_generalized_linear_regression(rescue_cat, 
                                            formula = "is_cat ~ engine_count + job_hours + hourly_cost + originofcall + propertycategory + specialservicetypecategory", 
                                            family = "binomial", 
                                            link = "logit")

# View a tibble of coefficients, std. error, p-values
broom::tidy(glm_out)

 
glm_out$summary$coefficient_standard_errors()

 
broom::tidy(glm_out) %>%
  dplyr::mutate(lower_ci = estimate - (1.96 * std.error),
                upper_ci = estimate + (1.96 * std.error))

 
glm_singular <- sparklyr::ml_generalized_linear_regression(rescue_cat, 
                                                 formula = "is_cat ~ typeofincident + engine_count + job_hours + hourly_cost + originofcall + propertycategory + specialservicetypecategory", 
                                                 family = "binomial", 
                                                 link = "logit")

 
rescue_cat %>% 
  dplyr::count(specialservicetypecategory) %>% 
  dplyr::arrange(n)

 
rescue_cat_ohe <- rescue_cat %>%
  dplyr::mutate(specialservicetypecategory = ifelse(specialservicetypecategory == "Other Animal Assistance",
                                                    "000_Other Animal Assistance",
                                                    specialservicetypecategory)) %>%
  sparklyr::ft_string_indexer(input_col = "specialservicetypecategory", 
                              output_col = "specialservicetypecategory_idx",
                              string_order_type = "alphabetDesc") %>%
  sparklyr::ft_one_hot_encoder(input_cols = c("specialservicetypecategory_idx"), 
                               output_cols = c("specialservicetypecategory_ohe"), 
                               drop_last = TRUE) %>%
  sparklyr::sdf_separate_column(column = "specialservicetypecategory_ohe",
                                into = c("specialservicetypecategory_Animal Rescue From Water", 
                                         "specialservicetypecategory_Animal Rescue From Below Ground", 
                                         "specialservicetypecategory_Animal Rescue From Height")) %>% 
  sparklyr::select(-ends_with(c("_ohe", "_idx")),
                   -specialservicetypecategory)

 
# originofcall
rescue_cat_ohe <- rescue_cat_ohe %>%
  dplyr::mutate(originofcall = ifelse(originofcall == "Person (mobile)",
                                      "000_Person (mobile)",
                                      originofcall )) %>%
  sparklyr::ft_string_indexer(input_col = "originofcall", 
                              output_col = "originofcall_idx",
                              string_order_type = "alphabetDesc") %>%
  sparklyr::ft_one_hot_encoder(input_cols = c("originofcall_idx"), 
                               output_cols = c("originofcall_ohe"), 
                               drop_last = TRUE) %>%
  sparklyr::sdf_separate_column(column = "originofcall_ohe",
                                into = c("originofcall_Ambulance", 
                                         "originofcall_Police", 
                                         "originofcall_Coastguard",
                                         "originofcall_Person (land line)",
                                         "originofcall_Not Known",
                                         "originofcall_Person (running Call)",
                                         "originofcall_Other Frs")) 
# propertycategory
rescue_cat_ohe <- rescue_cat_ohe %>%
  dplyr::mutate(propertycategory = ifelse(propertycategory == "Dwelling",
                                          "000_Dwelling",
                                          propertycategory)) %>%
  sparklyr::ft_string_indexer(input_col = "propertycategory", 
                              output_col = "propertycategory_idx",
                              string_order_type = "alphabetDesc") %>%
  sparklyr::ft_one_hot_encoder(input_cols = c("propertycategory_idx"), 
                               output_cols = c("propertycategory_ohe"), 
                               drop_last = TRUE) %>%
  sparklyr::sdf_separate_column(column = "propertycategory_ohe",
                                into = c("propertycategory_Outdoor", 
                                         "propertycategory_Road Vehicle", 
                                         "propertycategory_Non Residential",
                                         "propertycategory_Boat",
                                         "propertycategory_Outdoor Structure",
                                         "propertycategory_Other Residential"))


# remove _idx and _ohe intermediate columns and original data columns
# also remove 
 
# Run regression with one-hot encoded variables with chosen reference categories
glm_ohe <- sparklyr::ml_generalized_linear_regression(rescue_cat_ohe, 
                                                      formula = "is_cat ~ .", 
                                                      family = "binomial", 
                                                      link = "logit")

# Get coefficients and confidence intervals
broom::tidy(glm_ohe) %>%
  dplyr::mutate(lower_ci = estimate - (1.96 * std.error),
                upper_ci = estimate + (1.96 * std.error))


# Get feature column names
features <- rescue_cat_ohe %>%
  select(-is_cat) %>%
  colnames()

# Generate correlation matrix  
ml_corr(rescue_cat_ohe, columns = features, method = "pearson") %>%
  print()


rescue_cat %>%
  sparklyr::select(job_hours,
                   hourly_cost,
                   total_cost) %>%
  dplyr::arrange(desc(job_hours)) %>%
  print(n = 30)


rescue_cat <- rescue %>% 
# generate is_cat column
  dplyr::mutate(is_cat = ifelse(animal_group == "Cat", 1, 0)) %>% 
  sparklyr::select(engine_count, 
                   hourly_cost, 
                   originofcall, 
                   propertycategory,
                   specialservicetypecategory,
                   is_cat) %>%
# ensure numeric columns have the correct type
  dplyr::mutate(across(c(engine_count, hourly_cost), 
                   ~as.numeric(.))) %>%
# remove missing values
  sparklyr::filter(!is.na(engine_count)) %>%
# select reference categories and ensure they will be ordered last by the indexer
  dplyr::mutate(specialservicetypecategory = ifelse(specialservicetypecategory == "Other Animal Assistance",
                         "000_Other Animal Assistance",
                         specialservicetypecategory)) %>%
  dplyr::mutate(originofcall = ifelse(originofcall == "Person (mobile)",
                         "000_Person (mobile)",
                          originofcall )) %>%
  dplyr::mutate(propertycategory = ifelse(propertycategory == "Dwelling",
                         "000_Dwelling",
                         propertycategory))



ft_dplyr_transformer(sc, rescue_cat)


rescue_pipeline <- ml_pipeline(sc) %>%
  sparklyr::ft_dplyr_transformer(rescue_cat) %>%
  sparklyr::ft_string_indexer(input_col = "specialservicetypecategory", 
                    output_col = "specialservicetypecategory_idx",
                    string_order_type = "alphabetDesc") %>% 
  sparklyr::ft_string_indexer(input_col = "originofcall", 
                    output_col = "originofcall_idx",
                    string_order_type = "alphabetDesc") %>%
  sparklyr::ft_string_indexer(input_col = "propertycategory", 
                    output_col = "propertycategory_idx",
                    string_order_type = "alphabetDesc") %>%
  sparklyr::ft_one_hot_encoder(input_cols = c("specialservicetypecategory_idx"), 
                     output_cols = c("specialservicetypecategory_ohe"), 
                     drop_last = TRUE) %>%
  sparklyr::ft_one_hot_encoder(input_cols = c("originofcall_idx"), 
                     output_cols = c("originofcall_ohe"), 
                     drop_last = TRUE) %>%
  sparklyr::ft_one_hot_encoder(input_cols = c("propertycategory_idx"), 
                     output_cols = c("propertycategory_ohe"), 
                     drop_last = TRUE)  %>%
  sparklyr::ft_vector_assembler(input_cols = c("engine_count", "hourly_cost", "originofcall_ohe", "propertycategory_ohe",
                                              "specialservicetypecategory_ohe"), 
                                output_col = "features") %>%
  ml_generalized_linear_regression(features_col = "features",
                                   label_col = "is_cat",
                                   family = "binomial", 
                                   link = "logit") 
                                   
# View the pipeline
rescue_pipeline



# Fit the pipeline 
fitted_pipeline <- ml_fit(rescue_pipeline, rescue)

# View the fitted pipeline - notice output now shows model coeffiecients
fitted_pipeline

# Get predictions
predictions <- ml_transform(fitted_pipeline, rescue) 


# Access the logistic regression stage of the pipeline
model_details <- ml_stage(fitted_pipeline, 'generalized_linear_regression')

# Get coefficients and summary statistics from model
summary <- tibble(coefficients = c(model_details$intercept, model_details$coefficients), 
                  std_errors = model_details$summary$coefficient_standard_errors(), 
                  p_values = model_details$summary$p_values()) %>%
  dplyr::mutate(lower_ci = coefficients - (1.96 * std_errors),
                upper_ci = coefficients + (1.96 * std_errors))
                
# View the summary
summary



# Save pipeline
ml_save(
  rescue_pipeline,
  "rescue_pipeline",
  overwrite = TRUE
)

# Save pipeline model
ml_save(
  fitted_pipeline,
  "rescue_model",
  overwrite = TRUE
)


# Reload our saved pipeline
reloaded_pipeline <- ml_load(sc, "rescue_pipeline")

# Re-fit to a subset of rescue data as an example of how pipelines can be re-used
new_model <- ml_fit(reloaded_pipeline, sample_frac(rescue, 0.1))



# Close the spark session
spark_disconnect(sc)
