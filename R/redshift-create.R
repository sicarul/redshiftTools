#' Create redshift table
#' @param .data \code{data.frame}
#' @param table_name \code{character}
rs_create_table <- function(.data, table_name) {
  data_types <- recode(unlist(lapply(.data, class)),
         numeric = "FLOAT8",
         integer = "BIGINT",
         character = "VARCHAR(255)")
  spec <- paste(paste(names(.data), data_types), collapse=", ")
  sql_code <- whisker::whisker.render("CREATE TABLE IF NOT EXISTS {{table_name}} ({{spec}})")
  DBI::dbGetQuery(rs_db()$con, sql_code)
}
