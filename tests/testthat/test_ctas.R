context("ctas()")

test_that(
  "ctas works at all", {
  ttn <- temp_table_name()
  zapieR::rs_db("select 1 as foo") %>%
    ctas(ttn, temp = TRUE)
  expect_equivalent(zapieR::rs_db(ttn) %>%
                      collect, data.frame(foo=1L))
  DBI::dbExecute(zapieR::rs_db()$con, glue::glue("drop table {ttn}"))
})
