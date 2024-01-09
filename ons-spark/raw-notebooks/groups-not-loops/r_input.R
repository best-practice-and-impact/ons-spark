options(warn = -1)
library(sparklyr)
library(dplyr)

sc <- sparklyr::spark_connect(
  master = "local[2]",
  app_name = "groups-not-loops",
  config = sparklyr::spark_config(),
  )
  

rescue <- sparklyr::spark_read_csv(
  sc,
  path = "/training/animal_rescue.csv",
  header = TRUE,
  infer_schema = TRUE) %>% 
  sparklyr::select(
    AnimalGroup = AnimalGroupParent,
    TotalCost = IncidentNotionalCostGBP,
    Description = FinalDescription,
    CalYear = CalYear,
    IncidentNumber = IncidentNumber)


head(rescue) %>% 
  sparklyr::collect() %>%
  print()
  

rescue %>% 
  sparklyr::filter(AnimalGroup == "Cat") %>%
  count() %>%
  print()


animals = c("Cat", "Dog", "Hamster", "Deer")

result = list()

for (animal in animals) {
    count <- rescue %>%
      sparklyr::filter(AnimalGroup == animal) %>%
      count() %>%
      sparklyr::collect() %>%
      as.numeric()
  
    result <- append(result, count)
}


animal_counts <- data.frame(AnimalGroup = animals, count = unlist(result))

head(animal_counts)


rescue %>%
  sparklyr::filter(AnimalGroup %in% animals) %>%
  dplyr::group_by(AnimalGroup) %>%
  dplyr::summarise(count = n()) %>%
  dplyr::arrange(desc(count)) %>%
  sparklyr::collect() %>%
  print()


get_expensive_incidents <- function(df, animal) {
  df %>%
    sparklyr::filter(AnimalGroup == animal) %>%
    sparklyr::select(IncidentNumber, CalYear, TotalCost, Description, AnimalGroup) %>%
    dplyr::arrange(desc(TotalCost), IncidentNumber) %>%
    head(3)

}


result <- list() #create an empty list to store out results from the loop

for (animal in animals) {
  expensive_animal <- get_expensive_incidents(rescue, animal) #apply our function to each animal
  expensive_animal$AnimalGroup <- animal
  result <- append(result, list(expensive_animal)) #add result to list
}

expensive_rescues <- do.call(rbind, result) %>% #convert list into dataframe
  dplyr::arrange(AnimalGroup, desc(TotalCost))

expensive_rescues %>% 
  collect() %>%
  print()


get_expensive_incidents_grouped <- function(df, animals){
  df %>%
    sparklyr::filter(AnimalGroup %in% animals) %>%
    sparklyr::select(IncidentNumber, CalYear, TotalCost, Description, AnimalGroup) %>%
    dplyr::group_by(AnimalGroup) %>%
    dplyr::arrange(desc(TotalCost)) %>%
    sparklyr::filter(row_number() <= 3) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(AnimalGroup, desc(TotalCost), IncidentNumber)
}

expensive_rescues <- get_expensive_incidents_grouped(rescue, animals)

expensive_rescues %>% 
  collect() %>%
  print()


for (animal in animals) {
  rescue <- rescue %>%
    sparklyr::mutate(!!animal := ifelse(
      AnimalGroup == animal, TRUE, FALSE))
  
}

rescue %>%
  dplyr::arrange(IncidentNumber) %>%
  head(5) %>%
  collect() %>%
  print()

