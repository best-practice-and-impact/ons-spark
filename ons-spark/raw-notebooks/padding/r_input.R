options(warn = -1)
library(sparklyr)

sc <- sparklyr::spark_connect(
    master = "local[2]",
    app_name = "padding",
    config = sparklyr::spark_config())

config <- yaml::yaml.load_file("ons-spark/config.yaml")

rescue <- sparklyr::spark_read_parquet(sc, config$rescue_path) %>%
    sparklyr::filter(origin_of_call == "Police") %>%
    dplyr::arrange(date_time_of_call) %>%
    sparklyr::select(incident_number, origin_of_call)

rescue %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()    

pillar::glimpse(rescue)

rescue <- rescue %>%
    sparklyr::mutate(incident_no_length = length(incident_number))

rescue %>%
    dplyr::group_by(incident_no_length, origin_of_call) %>%
    dplyr::summarise(count = n()) %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

rescue <- rescue %>%
    sparklyr::mutate(padded_incident_no = lpad(incident_number, 9, "0"))

rescue %>%
    dplyr::arrange(incident_no_length) %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

rescue %>%
    dplyr::arrange(desc(incident_no_length)) %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

rescue <- rescue %>%
    sparklyr::mutate(too_short_inc_no = lpad(incident_number, 3, "0"))

rescue %>%
    dplyr::arrange(desc(incident_no_length)) %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

rescue <- rescue %>%
    sparklyr::mutate(silly_example = lpad(incident_number, 14, "xyz"))
    
rescue %>%
    dplyr::arrange(incident_no_length) %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()

rescue <- rescue %>%
    sparklyr::mutate(right_padded_inc_no = rpad(incident_number, 9, "0"))
    
rescue %>%
    dplyr::arrange(right_padded_inc_no) %>%
    sparklyr::select(incident_number, right_padded_inc_no) %>%
    head(5) %>%
    sparklyr::collect() %>%
    print()
