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
spectrum_add_partition <- function(dbcon, table_name, part_name, part_value, base_location) {
  stopifnot("ident" %in% class(table_name))

  # if the partition already exists, drop it
  table_parts <- strsplit(table_name, '.', fixed = TRUE)[[1]]
  matching_partitions <- nrow(dbGetQuery(dbcon, glue("select * from SVV_EXTERNAL_PARTITIONS where schemaname = '{table_parts[1]}' and tablename = '{table_parts[2]}' and values ILIKE '%{part_value}%'")))
  if (matching_partitions >= 1) {
    dbExecute(dbcon, glue("alter table {table_name} drop partition({part_name}='{part_value}')"))
  }

  dbExecute(dbcon, glue("alter table {table_name} add partition({part_name}='{part_value}') location '{base_location}/{part_name}={part_value}'"))
}
