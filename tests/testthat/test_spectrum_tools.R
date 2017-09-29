context("Spectrum Tools")

test_that("Don't allow a partionless schema with type date", {
  expect_error(define_external_table_spec(d1, dbplyr::in_schema("external_testing", demo_loc), local_spark$s3_url(demo_loc)))
})
