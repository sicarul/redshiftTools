#' Create redshift table
#' @param .data \code{data.frame}
#' @param table_name \code{character}
#' @export
rs_create_table <- function(.data, table_name) {
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
  spec <- paste(paste(names(.data), data_types), collapse=", ")
  sql_code <- whisker::whisker.render("CREATE TABLE IF NOT EXISTS {{table_name}} ({{spec}})")
  DBI::dbGetQuery(rs_db()$con, sql_code)
}
