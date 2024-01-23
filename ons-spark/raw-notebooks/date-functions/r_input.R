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


festive_days <- data.frame(
  "month_day" = c("Dec-24","Dec-25","Dec-26","Jan-01"),
  "festive_day" =c("Christmas Eve","Christmas Day","Boxing Day","New Years Day"))

festive_days <- copy_to(sc,festive_days)

festive_days

festive_rescues <- inner_join(rescue, festive_days,by = c("call_month_day" = "month_day"))  %>% 
  select(-c(call_month_day,call_month_day))

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

rescue <- rescue %>%
  mutate(adjusted_month_day = date_format(adjusted_date, "MMM-dd"))

orthodox_festive_rescues <- inner_join(rescue, festive_days,by = c("adjusted_month_day" = "month_day"))  %>% 
  dplyr::rename(orthodox_festive_day = festive_day)  %>%
  filter(orthodox_festive_day != "Boxing Day") %>%
  select(incident_number ,date_of_call ,orthodox_festive_day)
    
print(orthodox_festive_rescues, n = 5)

orthodox_festive_rescues %>% 
  group_by(date_of_call,orthodox_festive_day) %>%
  summarize(count = n()) %>% 
  arrange(date_of_call) %>%
  print(n = 10)
