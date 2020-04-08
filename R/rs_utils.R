#' List redshift tables
#'
#' Lists all tables available in redshift database
#'
#' @param dbcon an RPostgres/RJDBC connection to the redshift server
#'
#' @return a data.frame of table names
#' @export
rs_list_tables <- function(dbcon) {
  dbGetQuery(dbcon, "select schemaname as schema, tablename as table,schemaname || '.' || tablename as full_name from pg_tables")
}


#' Show table information
#'
#' @param dbcon an RPostgres/RJDBC connection to the redshift server
#' @param table table name
#' @param schema schema. defaults to public

#'
#' @return a data.frame containing table information
#' @export
rs_table_def <- function(dbcon,table,schema='public') {

  df <- dbGetQuery(dbcon,sprintf("set search_path to %s; select * from pg_table_def where tablename = '%s'",schema,table))

  return(df)
}
