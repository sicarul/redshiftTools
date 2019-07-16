#' List redshift tables
#'
#' Lists all tables available in redshift database
#'
#' @param dbcon an RPostgres/RJDBC connection to the redshift server
#' @param pattern regular expression to match subset. defaults to all tables
#'
#' @return a character vector of table names in the format {schema name}.{table name}
#' @export
rs_list_tables <- function(dbcon, pattern='.*') {
  grep(
    pattern,
    dbGetQuery(dbcon, "select schemaname || '.' || tablename from pg_tables")[[1]],
    value = T
  )

}


#' Show table information
#'
#' @param dbcon an RPostgres/RJDBC connection to the redshift server
#' @param tablename redshift table in the format {schema name}.{table name}
#'
#' @return a data.frame containing table information
#' @export
rs_table_def <- function(dbcon,tablename) {
  split <- strsplit(tablename,split='\\.')[[1]]
  schemaname <- split[[1]]
  tablename <- split[[2]]

  df <- dbGetQuery(dbcon,sprintf("set search_path to %s; select * from pg_table_def where tablename = '%s'",schemaname,tablename))

  return(df)
}
