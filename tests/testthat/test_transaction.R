context("transaction()")

zapieR::make_db_connections()

## Destructive test defered
# DBI::dbGetQuery(conn = rs$con, statement = "drop table if exists mtcars;")
#
#
# test_that("transactions works at all", {
#   expect_true({
#     transaction(
#       .data = mtcars,
#       .dbcon = rs$con,
#       .function_sequence = list(
#         function(...) { rs_create_table(table_name = "mtcars", ...) },
#         function(...) { rs_upsert_table(tableName = "mtcars", ...) },
#         function(...) { rs_replace_table(tableName = "mtcars", ...) }
#       )
#     )
#   })
#
#   uploaded_mtcars <- function() { DBI::dbGetQuery(rs$con, "select * from mtcars") }
#
#   expect_equal(dim(uploaded_mtcars()), dim(mtcars))
# })
#
# DBI::dbGetQuery(conn = rs$con, statement = "drop table if exists mtcars;")
#
