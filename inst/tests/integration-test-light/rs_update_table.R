library(zapieR)
library(DBI)
library(redshiftTools)
library(magrittr)
library(testthat)

make_db_connections()

context("rs_upsert_table")

create_test_data <- function() {
  dbGetQuery(rs_db()$con, "drop table if exists test_cars")
  cars %>% rs_create_table(rs_db()$con, "test_cars")
  cars %>% rs_upsert_table(rs_db()$con, "test_cars", bucket = "zapier-data-science-storage")
  return("test_cars")
}

test_that("Can upsert a table", {
  create_test_data()
  testthat::expect_true(nrow(anti_join(cars, rs_db("test_cars") %>% collect(n = Inf)))==0, label = "All redshift test_cars rows in cars")
  testthat::expect_true(nrow(anti_join(rs_db("test_cars") %>% collect(n = Inf), cars))==0, label = "All cars rows in redshift test_cars")
})

test_that("Inserted values with keys doesn't increase number of entries", {
  create_test_data()
  cars %>% rs_upsert_table(rs_db()$con, "test_cars", bucket = "zapier-data-science-storage", keys = c("speed", "dist"))
  testthat::expect_true(nrow(cars) == (rs_db("test_cars") %>% collect(n = Inf) %>% nrow), label = "Inserted values with keys doesn't increase number of entries")
})

test_that("Inserted values with a key can increase number of entries", {
  create_test_data()
  cars %>% rs_upsert_table(rs_db()$con, "test_cars", bucket = "zapier-data-science-storage", keys = c("dist"))
  testthat::expect_true(nrow(cars) != (rs_db("test_cars") %>% collect(n = Inf) %>% nrow), label = "Inserted values with a key can increase number of entries")
})

test_that("Inserted values without a key is just a dumb insert", {
  create_test_data()
  cars %>% rs_upsert_table(rs_db()$con, "test_cars", bucket = "zapier-data-science-storage")
  testthat::expect_true(nrow(cars)*2 == (rs_db("test_cars") %>% collect(n = Inf) %>% nrow))
})


test_that("Dynamic columns, strict = FALE", {
  dbGetQuery(rs_db()$con, "drop table if exists iris")
  iris$Species <- as.character(iris$Species)
  names(iris) <- gsub(".", "_", names(iris), fixed = TRUE)

  # the original dat is missing one column
  original <- iris[1:10, -1]
  original %T>%
    rs_create_table(rs_db()$con, "iris") %>%
    rs_upsert_table(rs_db()$con, "iris",bucket = "zapier-data-science-storage")

  # the new data is missing a different column
  new <- iris[11:20,-2]
  original %T>%
    rs_create_table(rs_db()$con, "iris") %>%
    rs_upsert_table(rs_db()$con, "iris",bucket = "zapier-data-science-storage")

  new %>%
    rs_upsert_table(rs_db()$con, "iris",bucket = "zapier-data-science-storage", strict = FALSE)

  testthat::expect_true(all(rs_db("iris") %>% collect %>% names %in% tolower(names(iris))))
  testthat::expect_true(all(tolower(names(iris)) %in% (rs_db("iris") %>% collect %>% names)))

  allOfEm <- bind_rows(new,original)
  names(allOfEm)  <- tolower(names(allOfEm))

  testthat::expect_true(rs_db("iris") %>% collect(n = Inf) %>%
    anti_join(bind_rows(allOfEm)) %>% nrow == 0)

  testthat::expect_true(allOfEm %>%
    anti_join(rs_db("iris") %>% collect(n = Inf)) %>% nrow == 0)
})
