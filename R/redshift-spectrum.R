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

#' Define the DDL for an External Table
#'
#' @param d data.frame
#' @param table_name Result from dbplyr::in_schema specifying the table and the schema
#' @param location s3:// style url
#' @param partitioned_by
#'
#' @return
#' @export
#' @importFrom dbplyr in_schema

define_external_table_spec <- function(d, table_name, location, partitioned_by = "") {
  # Check table name
  if (grepl("-", table_name, fixed = TRUE)) {
    stop("Hyphen in table name not allowed")
  }
  redshift_types <- identify_rs_types(d)
  redshift_colnames <- sanitize_column_names_for_redshift(d)
  # Find index location of the columns we are partitioning by
  partitioned_by_index <- which(names(d) %in% partitioned_by)

  # We have to test seperately for the DATE type because a negative 0 length index returns a null vector
  if (partitioned_by == "") {
    if ("DATE" %in% redshift_types) {
      stop("The DATE data type is only valid for partitioning columns, include it in your partition or recast to timestamp")
    }
    partitioned_by_spec <- ""

  } else {
    # The 'date' type is only valid for partitioning columns.
    if ("DATE" %in% redshift_types[-partitioned_by_index]) {
      stop("The DATE data type is only valid for partitioning columns, include it in your partition or recast to timestamp")
    }
    partitioned_by_spec <- paste0("PARTITIONED BY (",paste0(paste0(redshift_colnames[partitioned_by_index], " ", redshift_types[partitioned_by_index]), collapse = ","),")", collapse = "")
    redshift_colnames <- redshift_colnames[-partitioned_by_index]
    redshift_types <- redshift_types[-partitioned_by_index]
  }

  assertthat::assert_that("ident" %in% class(table_name), msg = "table_name must be result of dbplyr::in_schema()")
  column_specification <- paste0(paste0(redshift_colnames, " ", redshift_types), collapse = ",")
  glue("CREATE EXTERNAL TABLE {table_name} ({column_specification}) {partitioned_by_spec} STORED AS parquet LOCATION '{location}'")
}
