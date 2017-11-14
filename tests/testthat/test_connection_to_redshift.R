context("Test that we can make a connection to Redshift")

zapieR::make_db_connections()

test_that(
  "We can make a connection to Redshift",
  expect_equal(
    nrow(DBI::dbGetQuery(rs$con, "select * from event limit 1")),
    1
  )
)
