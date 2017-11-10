context("redshift-utils.R")

test_that("can decompose in_schema", {
  eg <- redshiftTools::decompose_in_schema(dbplyr::in_schema("s", "t"))
  expect_equal(attr(eg, "table_name"), "t")
  expect_equal(attr(eg, "schema_name"), "s")
})

test_that("correctly infer numeric type", {
  expect_true(all(identify_rs_types(cars) == "FLOAT8"))
})

test_that("correctly handle factor type", {
  expect_true(suppressWarnings(all(identify_rs_types(iris) == c("FLOAT8", "FLOAT8", "FLOAT8", "FLOAT8", "VARCHAR(12)"))))
})

test_that("correctly handle character type", {
  dat <- iris
  dat$Species <- zapieR::unfactor(dat$Species)
  expect_type(dat$Species, "character")
  expect_true(suppressWarnings(all(identify_rs_types(dat) == c("FLOAT8", "FLOAT8", "FLOAT8", "FLOAT8", "VARCHAR(12)"))))
})

test_that("correctly handle time types", {
  dat <- data.frame(
    date = as.Date(lubridate::now()),
    posixCT = zapieR::makePOSIXct(lubridate::now()),
    posixLT = as.POSIXlt(zapieR::makePOSIXct(lubridate::now()))
  )
  expect_true(suppressWarnings(all(identify_rs_types(dat) == c("DATE", "TIMESTAMP", "TIMESTAMP"))))
})
