#'Group by Animal
#
#'Performs a simple groupBy and count on the DF
#
#'Uses initcap() on animal_group, which capitalises the first letter
#'    of each word, to remove data input errors. Note that this does
#'    not happen to the input animal, so "cat" and "CAT" will always
#'    return a count of 0. initcap() is a Spark function and can
#'    only be used on a sparklyr DataFrame; more details:
#'    https://best-practice-and-impact.github.io/ons-spark/sparklyr-intro/sparklyr-functions.html
#'        
#'@param sdf A sparklyr DataFrame with a column named animal_group
#'@returns A sparklyr DataFrame with animal_group and count
group_animal <- function(sdf){
    
    sdf <- sdf %>%
        sparklyr::mutate(animal_group = initcap(animal_group)) %>%
        dplyr::group_by(animal_group) %>%
        dplyr::summarise(count = n())
    
    return (sdf)
}