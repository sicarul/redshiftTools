# context("Spectrum Tools")
#
# zapieR::make_db_connections()
#
## Can't retest creation without being destructive, so test canceled
d1 <- cars
d1$moo <- lubridate::today()

test_that("Don't allow a partionless schema with type date", {
  expect_error(
    create_external_table(con = rs$con, d = d1, table_name = dbplyr::in_schema("external_testing", "moo"), location = "nope")
  )
})
