#'Animal Cost
#
#'Sums the TotalCost for number of "animal" in the AnimalGroup column
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
#'@returns sum of TotalCost of animal in animal_group
animal_cost <- function(sdf, animal){
    
    sdf <- sdf %>%
        sparklyr::mutate(animal_group = initcap(animal_group)) %>%
        sparklyr::filter(animal_group == animal) %>%
        dplyr::summarise(total_cost = sum(total_cost, na.rm = TRUE))
    
    return (sdf)
}