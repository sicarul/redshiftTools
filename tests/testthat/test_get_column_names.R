context("test_get_column_names()")

ttn <- temp_table_name()
on.exit(try(DBI::dbExecute(zapieR::rs_db()$con, glue("DROP TABLE {ttn}"))), add = TRUE)

test_that("can get column names from empty table", {
  rs_create_table(cars, zapieR::rs_db()$con, table_name = ttn)
  expect_equivalent(get_column_names(con = zapieR::rs_db()$con, ttn), c("speed", "dist"))
})

test_that("can get column names from populated table", {
  rs_replace_table(data = cars, dbcon = zapieR::rs_db()$con, tableName = ttn, bucket = "zapier-data-science-storage")
  expect_equivalent(suppressWarnings(get_column_names(con = zapieR::rs_db()$con, ttn), c("speed", "dist")))
})

test_that("can get column names from populated table with schema", {
  rs_replace_table(data = cars, dbcon = zapieR::rs_db()$con, tableName = ttn, bucket = "zapier-data-science-storage")
  expect_equivalent(get_column_names(con = zapieR::rs_db()$con, dbplyr::in_schema("public", ttn)), c("speed", "dist"))
})

test_that("can get column names from temp table", {
  ttn2 <- temp_table_name()
  zapieR::rs_db(ttn) %>%
    ctas(ttn2, temp = TRUE)
  expect_equivalent(get_column_names(con = zapieR::rs_db()$con, ttn2), c("speed", "dist"))
})


