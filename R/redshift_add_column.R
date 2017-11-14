#' Add a column to a redshift table
#'
#' Add a typed column to a redshift database
#'
#' @param dbcon Database connection object of type RPostgreSQL
#' @param table_name the name of the target table (character)
#' @param column_name (character)
#' @param redshift_type (character) for the redshift type to assign for this column
#'
#' @return NULL
#' @export
rs_add_column <- function(dbcon, table_name, column_name, redshift_type) {
  if (!column_name %in% DBI::dbListFields(dbcon, table_name)) {
    DBI::dbGetQuery(
      dbcon,
      whisker.render("alter table {{table_name}}
       add column {{column_name}} {{redshift_type}}
       default NULL", list(table_name = table_name, column_name = column_name, redshift_type = redshift_type))
    )
  }
  return(NULL)
}
