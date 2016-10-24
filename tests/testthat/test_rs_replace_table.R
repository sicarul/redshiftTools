context("rs_replace_table()")

zapieR::make_db_connections()
DBI::dbGetQuery(conn = rs$con, statement = "drop table if exists mtcars;")
rs_create_table(.data = mtcars, dbcon = rs$con, table_name = "mtcars")

test_that(
  "The table mtcars can be replaced on Redshift", {
    uploaded_mtcars <- function() { DBI::dbGetQuery(rs$con, "select * from mtcars") }
    expect_true(suppressMessages({ rs_replace_table(mtcars, rs$con, "mtcars") }))
    expect_equal(uploaded_mtcars() %>% names(), names(mtcars))
    expect_equal(dim(uploaded_mtcars()), dim(mtcars))
  }
)

DBI::dbGetQuery(conn = rs$con, statement = "drop table if exists mtcars;")

