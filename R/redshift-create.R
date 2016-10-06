identify_rs_types <- function(.data) {
  classes <- lapply(.data, class)
  classes_first_pass <- lapply(classes, function(x) {
    if (all(c("POSIXct", "POSIXt") %in% x)) {
      x <- "TIMESTAMP"
    }
    return(x)
  })
  if (any("factor" %in% classes_first_pass)) {
    warning("one of the columns is a factor")
  }
  data_types <- recode(unlist(classes_first_pass),
         factor = "VARCHAR(255)",
         numeric = "FLOAT8",
         integer = "BIGINT",
         character = "VARCHAR(255)")
  return(data_types)
}

#' Create redshift table
#' @param .data \code{data.frame}
#' @param table_name \code{character}
#'
#' @return The column names that redshift actually ended up using
#' @export
rs_create_table <- function(.data, dbcon, table_name) {
  data_types <- identify_rs_types(.data)
  # Identify and mutate column names
  column_names <- names(.data)
  column_name_is_reserved <- column_names %in% tolower(RESERVED_WORDS)
  column_names[column_name_is_reserved] <- paste0('rw_', column_names[column_name_is_reserved])

  spec <- paste(paste(column_names, data_types), collapse=", ")
  sql_code <- whisker::whisker.render("CREATE TABLE IF NOT EXISTS {{table_name}} ({{spec}})")
  DBI::dbGetQuery(dbcon, sql_code)
  return(column_names)
}
