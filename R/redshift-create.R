#' Create redshift table
#' @param .data \code{data.frame}
#' @param table_name \code{character}
#'
#' @return The column names that redshift actually ended up using
#' @importFrom DBI dbExecute
#' @importFrom whisker whisker.render
#' @export
rs_create_table <- function(.data, dbcon, table_name, ...) {
  warnifnoschema(table_name)
  data_types <- identify_rs_types(.data)
  # Check table name
  if (grepl("-", table_name, fixed = TRUE)) {
    stop("Hyphen in table name not allowed")
  }
  # Identify and mutate column names
  column_names <- sanitize_column_names_for_redshift(names(.data))

  spec <- paste(paste(column_names, data_types), collapse=", ")
  sql_code <- whisker::whisker.render("CREATE TABLE IF NOT EXISTS {{table_name}} ({{spec}})")
  DBI::dbExecute(dbcon, sql_code)
  return(column_names)
}
