#' Add a partition to Redshift Spectrum
#'
#' @param table_name A dblyr::in_schema defined table name
#' @param part_name The name of the partition
#' @param part_value The value of the partition
#' @param base_location The base s3:// location of the partition files. It is assumed that the partition is specified as {base_location}/{part_name}={part_value}
#'
#' @return The result of the dbExecute for the command
#' @export
#' @importFrom glue glue
#' @importFrom DBI dbExecute
add_partition <- function(dbcon, table_name, part_name, part_value, base_location) {
  stopifnot("ident" %in% class(table_name))
  dbExectue(dbcon, paste0(glue("alter table {table_name} drop partition({part_name}='{part_value}');"),
  glue("alter table {table_name} add partition({part_name}='{part_value}') location '{base_location}/{part_name}={part_value}'")))
}
