context("redshift-utils.R")

test_that("correctly infer numeric type", {
  expect_true(all(identify_rs_types(cars) == "FLOAT8"))
})

test_that("correctly handle factor type", {
  expect_warning(identify_rs_types(iris))
  expect_true(suppressWarnings(all(identify_rs_types(iris) == c("FLOAT8", "FLOAT8", "FLOAT8", "FLOAT8", "VARCHAR(11)"))))
})

test_that("correctly handle character type", {
  dat <- iris
  dat$Species <- unfactor(dat$Species)
  expect_type(dat$Species, "character")
  expect_true(suppressWarnings(all(identify_rs_types(dat) == c("FLOAT8", "FLOAT8", "FLOAT8", "FLOAT8", "VARCHAR(11)"))))
})

test_that("correctly handle time types", {
  dat <- data.frame(
    date = as.Date(now()),
    posixCT = zapieR::makePOSIXct(lubridate::now()),
    posixLT = as.POSIXlt(makePOSIXct(lubridate::now()))
  )
  expect_true(suppressWarnings(all(identify_rs_types(dat) == c("DATE", "TIMESTAMP", "TIMESTAMP"))))
})

