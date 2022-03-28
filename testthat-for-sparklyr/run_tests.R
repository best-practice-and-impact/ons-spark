# Set the working directory if not already set
test_wd <- "/ons-spark/testthat-for-sparklyr"

if(stringr::str_sub(getwd(), -nchar(test_wd)) != test_wd){
    setwd(paste0("./", test_wd))
}

# Run all the tests
testthat::test_dir("./tests")