context("rs_upsert_table()")

zapieR::make_db_connections()

test_that("We can insert a row into an existing table", {
  get_n <- function() {
    dbGetQuery(rs$con, "select count(*) as n from iris") %>% dplyr::pull(n)
  }
  original_n <- get_n()
  rs_upsert_table(iris[1, ], dbcon = rs$con, tableName = "iris", bucket = "zapier-data-science-storage")
  expect_gte(get_n(), original_n + 1)
})

## These tests are commented out because they are destructive
DBI::dbGetQuery(conn = rs$con, statement = "drop table if exists mtcars_with_id;")
mtcars_with_id <- cbind(id = 1:nrow(mtcars), mtcars)
rs_create_table(
  .data = mtcars_with_id,
  dbcon = rs$con, table_name = "mtcars_with_id"
)
#
# test_that(
# "When the table exists but is empty, rs_upsert_table works", {
# uploaded_mtcars <- function() { DBI::dbGetQuery(rs$con, "select * from mtcars_with_id")}
# expect_equal(0, nrow(uploaded_mtcars()))
# suppressMessages({
#     rs_upsert_table(mtcars_with_id, rs$con, "mtcars_with_id", keys = "id", use_transaction = FALSE, bucket = "zapier-data-science-storage")}
# )
# suppressMessages({
#     rs_upsert_table(mtcars_with_id, rs$con, "mtcars_with_id", keys = "id", bucket = "zapier-data-science-storage")}
# )
# suppressMessages({
#     # since this is upsert and we're giving a key, this operation should not
#     # duplicate rows.
#     transaction(.data = mtcars_with_id,
#     .dbcon = rs$con,
#     .function_sequence = list(function(...) { rs_upsert_table(tableName = "mtcars_with_id", keys = "id" , bucket = "zapier-data-science-storage", ...)})
#     )
# })
# expect_equal(dim(uploaded_mtcars()), dim(mtcars_with_id))
# DBI::dbGetQuery(rs$con, "delete from mtcars_with_id where mpg > 20")
# expect_equal(dim(uploaded_mtcars()), dim(mtcars_with_id[! mtcars_with_id$mpg > 20,]))
# expect_true(suppressMessages({
#     rs_upsert_table(mtcars_with_id, rs$con, "mtcars_with_id", key = "id")
# }))
# expect_null(suppressMessages({
#     rs_upsert_table(mtcars_with_id, rs$con, "mtcars_with_id", key = "id", use_transaction = FALSE)
# }))
# expect_equal(dim(uploaded_mtcars()), dim(mtcars_with_id))
# }
# )
# DBI::dbGetQuery(conn = rs$con, statement = "drop table if exists mtcars_with_id;")
# create_test_data <- function() {
#     dbGetQuery(rs_db()$con, "drop table if exists test_cars")
#     cars %>% rs_create_table(rs_db()$con, "test_cars")
#     cars %>% rs_upsert_table(rs_db()$con, "test_cars", bucket = "zapier-data-science-storage")
#     return("test_cars")
# }
# test_that("Can upsert a table", {
#     create_test_data()
#     testthat::expect_true(nrow(anti_join(cars, rs_db("test_cars") %>% collect(n = Inf))) == 0, label = "All redshift test_cars rows in cars")
#     testthat::expect_true(nrow(anti_join(rs_db("test_cars") %>% collect(n = Inf), cars)) == 0, label = "All cars rows in redshift test_cars")
# })
# test_that("Inserted values with keys doesn't increase number of entries", {
#     create_test_data()
#     cars %>% rs_upsert_table(rs_db()$con, "test_cars", bucket = "zapier-data-science-storage", keys = c("speed", "dist"))
#     testthat::expect_true(nrow(cars) == (rs_db("test_cars") %>% collect(n = Inf) %>% nrow), label = "Inserted values with keys doesn't increase number of entries")
# })
# test_that("Inserted values with a key can increase number of entries", {
#     create_test_data()
#     cars %>% rs_upsert_table(rs_db()$con, "test_cars", bucket = "zapier-data-science-storage", keys = c("dist"))
#     testthat::expect_true(nrow(cars) != (rs_db("test_cars") %>% collect(n = Inf) %>% nrow), label = "Inserted values with a key can increase number of entries")
# })
# test_that("Inserted values without a key is just a dumb insert", {
#     create_test_data()
#     cars %>% rs_upsert_table(rs_db()$con, "test_cars", bucket = "zapier-data-science-storage")
#     testthat::expect_true(nrow(cars) * 2 == (rs_db("test_cars") %>% collect(n = Inf) %>% nrow))
# })
# test_that("Dynamic columns, strict = FALE", {
#     dbGetQuery(rs_db()$con, "drop table if exists iris")
#     iris$Species <- as.character(iris$Species)
#     names(iris) <- gsub(".", "_", names(iris), fixed = TRUE)
#     #the original dat is missing one column
#     original <- iris[1 : 10, - 1]
#     original %T>%
#     rs_create_table(rs_db()$con, "iris") %>%
#     rs_upsert_table(rs_db()$con, "iris", bucket = "zapier-data-science-storage")
#     #the new data is missing a different column
#     new <- iris[11 : 20, - 2]
#     original %T>%
#     rs_create_table(rs_db()$con, "iris") %>%
#     rs_upsert_table(rs_db()$con, "iris", bucket = "zapier-data-science-storage")
#     new %>%
#     rs_upsert_table(rs_db()$con, "iris", bucket = "zapier-data-science-storage", strict = FALSE)
#     testthat::expect_true(all(rs_db("iris") %>% collect %>% names %in% tolower(names(iris))))
#     testthat::expect_true(all(tolower(names(iris)) %in% (rs_db("iris") %>% collect %>% names)))
#     allOfEm <- bind_rows(new, original)
#     names(allOfEm) <- tolower(names(allOfEm))
#     testthat::expect_true(rs_db("iris") %>%
#         collect(n = Inf) %>%
#         anti_join(bind_rows(allOfEm)) %>%
#         nrow == 0)
#     testthat::expect_true(allOfEm %>%
#         anti_join(rs_db("iris") %>% collect(n = Inf)) %>%
#         nrow == 0)
# })
