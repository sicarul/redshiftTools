context("rs_create_table")

zapieR::make_db_connections()
DBI::dbGetQuery(conn = rs$con, statement = "drop table if exists mtcars;")

test_that(
  "The table mtcars can be created and filled on Redshift", {
    expect_equal(
      rs_create_table(.data = mtcars, dbcon = rs$con, table_name = "mtcars"),
      names(mtcars)
    )

    suppressMessages(rs_upsert_table(mtcars, rs$con, "mtcars", bucket = "zapier-data-science-storage", use_transaction = FALSE))

    uploaded_mtcars <- DBI::dbGetQuery(rs$con, "select * from mtcars")
    expect_equal(
      uploaded_mtcars %>% names(), names(mtcars)
    )
    expect_equal(nrow(uploaded_mtcars), nrow(mtcars))
  }
)

DBI::dbGetQuery(conn = rs$con, statement = "drop table if exists mtcars;")

