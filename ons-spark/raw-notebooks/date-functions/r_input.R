options(warn = -1) 

library(sparklyr)
library(dplyr)

default_config <- sparklyr::spark_config()

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "date-functions",
    config = default_config)

rescue <- sparklyr::spark_read_csv(
    sc,
    path="/training/animal_rescue.csv", 
    header=TRUE,
    infer_schema=TRUE) %>%
    rename(
        incident_number = IncidentNumber,
        date_time_of_call = DateTimeOfCall,
       )  %>%
    select(incident_number, date_time_of_call)

rescue
 
rescue <- rescue %>% mutate(date_of_call = to_date(date_time_of_call, "dd/MM/yyyy"))  %>% 
      select(-c(date_time_of_call))

print(rescue, n = 5)

rescue <- rescue %>%
  mutate(call_month_day = date_format(date_of_call, "MMM-dd"))

print(rescue, n = 5)

festive_rescues <- rescue %>%
  mutate(festive_day = case_when(
      call_month_day == "Dec-24" ~  "Christmas Eve",
      call_month_day == "Dec-25" ~   "Christmas Day",
      call_month_day == "Dec-26" ~  "Boxing Day",
      call_month_day == "Jan-01" ~  "New Years Day",
    .default = NA)) %>%
  filter(!is.na(festive_day))

print(festive_rescues, n = 5 )

 
festive_rescues %>% 
  group_by(date_of_call,festive_day) %>%
  summarize(count = n()) %>% 
  arrange(date_of_call) %>%
  print(n = 10)

rescue %>%
  mutate(report_date = sql("date_of_call + interval 1 year 3 months")) %>%
  print(n = 5)

rescue <- rescue %>%
  mutate(adjusted_date = sql("date_of_call - interval 13 days"))
  
print(rescue, n = 5 )

orthodox_festive_rescues <- rescue %>%
  mutate(adjusted_month_day = date_format(adjusted_date, "MMM-dd")) %>%

  mutate(orthodox_festive_day = case_when(
      adjusted_month_day == "Dec-24" ~  "Christmas Eve",
      adjusted_month_day == "Dec-25" ~   "Christmas Day",
      adjusted_month_day == "Jan-01" ~  "New Years Day",
    .default = NA)) %>%
  filter(!is.na(orthodox_festive_day))
    
print(orthodox_festive_rescues, n = 5)

orthodox_festive_rescues %>% 
  group_by(date_of_call,orthodox_festive_day) %>%
  summarize(count = n()) %>% 
  arrange(date_of_call) %>%
  print(n = 10)
