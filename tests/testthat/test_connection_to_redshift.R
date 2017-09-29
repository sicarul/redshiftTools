Sys.setenv(REDSHIFT_ROLE = 'arn:aws:iam::996097627176:role/production-redshift')
context("Test that we can make a connection to Redshift")

zapieR::make_db_connections()

test_that(
  "We can make a connection to Redshift",
  expect_equal(
    nrow(DBI::dbGetQuery(rs$con, "select * from event limit 1")),
    1)
)
