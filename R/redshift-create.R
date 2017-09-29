#' Create redshift table
#' @param .data \code{data.frame}
#' @param table_name \code{character}
#'
#' @return The column names that redshift actually ended up using
#' @importFrom DBI dbExecute
#' @importFrom whisker whisker.render
#' @export
rs_create_table <- function(.data, dbcon, table_name, ...) {
  data_types <- identify_rs_types(.data)
  # Check table name
  if (grepl("-", table_name, fixed = TRUE)) {
    stop("Hyphen in table name not allowed")
  }
  # Identify and mutate column names
  column_names <- names(.data)
  column_name_is_reserved <- column_names %in% tolower(RESERVED_WORDS)
  column_names[column_name_is_reserved] <- paste0('rw_', column_names[column_name_is_reserved])

  spec <- paste(paste(column_names, data_types), collapse=", ")
  sql_code <- whisker::whisker.render("CREATE TABLE IF NOT EXISTS {{table_name}} ({{spec}})")
  DBI::dbExecute(dbcon, sql_code)
  return(column_names)
}
