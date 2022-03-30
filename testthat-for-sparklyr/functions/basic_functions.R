#'Count Animal
#
#'Counts the number of "animal" in the animal_group column
#
#'Uses initcap() on animal_group, which capitalises the first letter
#'    of each word, to remove data input errors. Note that this does
#'    not happen to the input animal, so "cat" and "CAT" will always
#'    return a count of 0. initcap() is a Spark function and can
#'    only be used on a sparklyr DataFrame; more details:
#'    https://best-practice-and-impact.github.io/ons-spark/sparklyr-intro/sparklyr-functions.html
#'        
#'@param sdf A sparklyr DataFrame
#'@param animal name of the animal to count
#'@returns count of animal in animal_group
count_animal <- function(sdf, animal){
    
    sdf <- sdf %>%
        sparklyr::mutate(animal_group = initcap(animal_group)) %>%
        sparklyr::filter(animal_group == animal) %>%
        dplyr::summarise(n())
    
    return (sdf)
}

#'Format Rescue Columns
#'
#'Changes the names of some columns to be briefer or more descriptive.
#'    
#'PumpCount renamed engine_count, final_description
#'    renamed description, IncidentNumber renamed incient_number
#'    and the data in AnimalGroupParent has the first letter of
#'    each word capitalised and is renamed animal_group.
#'
#'@param sdf A sparklyr DataFrame
#'@returns sparklyr DataFrame with columns renamed and reordered
format_columns <- function(sdf){
    
    sdf <- sdf %>%
        dplyr::rename(
            incident_number = IncidentNumber,
            engine_count = PumpCount,
            description = FinalDescription) %>%
        sparklyr::mutate(animal_group = initcap(AnimalGroupParent)) %>%
        sparklyr::select(incident_number, animal_group, engine_count, description)
    
    return (sdf)
}