table_parts <- function(table_name) {
  return(strsplit(table_name, ".", fixed = TRUE)[[1]])
}

globalVariables(c("V1", "values")) # suppresses check note due to NSE
#' List Spectrum partitions
#'
#' Provides character vector of partition names for a given table
#'
#' @param dbcon regular db connection object
#' @param table_name dbplyr::in_schema specification of table name
#'
#' @importFrom glue glue
#' @importFrom DBI dbGetQuery
#' @importFrom magrittr %>%
#' @importFrom dplyr pull
#' @importFrom jsonlite stream_in
#' @importFrom zapieR unfactor
#' @export
spectrum_list_partitions <- function(dbcon, table_name) {
  stopifnoschema(table_name)
  this_table_parts <- table_parts(table_name)
  dbGetQuery(dbcon, glue("select values from SVV_EXTERNAL_PARTITIONS where schemaname = '{this_table_parts[1]}' and tablename = '{this_table_parts[2]}'")) %>%
    pull(values) %>%
    textConnection() %>%
    jsonlite::stream_in(verbose = FALSE) %>% # the factor = 'string' arg didn't seem to work
    pull(V1) %>%
    unfactor()
}

#' Remove a partition from Redshift Spectrum
#'
#' @param dbcon The database connection to Redshift
#' @param table_name A dblyr::in_schema defined table name
#' @param part_name The name of the partition
#' @param part_value The value of the partition
#' @export
#' @importFrom glue glue
#' @importFrom DBI dbExecute
spectrum_drop_partition <- function(dbcon, table_name, part_name, part_value) {
  stopifnoschema(table_name)

  # if the partition already exists, drop it
  this_table_parts <- table_parts(table_name)

  matching_partitions <- nrow(dbGetQuery(dbcon, glue("select * from SVV_EXTERNAL_PARTITIONS where schemaname = '{this_table_parts[1]}' and tablename = '{this_table_parts[2]}' and values ILIKE '%{part_value}%'")))
  if (matching_partitions >= 1) {
    sql_code <- glue("alter table {table_name} drop partition({part_name}='{part_value}')")
    log_if_verbose("Dropping partition: ", sql_code)
    dbExecute(dbcon, sql_code)
  } else {
    log_if_verbose(glue("Partition already absent, {part_name}='{part_value}'"))
  }
  NULL
}


#' Add a partition to Redshift Spectrum
#'
#' @param dbcon The database connection to Redshift
#' @param table_name A dblyr::in_schema defined table name
#' @param part_name The name of the partition
#' @param part_value The value of the partition
#' @param base_location The base s3:// location of the partition files. It is assumed that the partition is specified as {base_location}/{part_name}={part_value}
#'
#' @return NULL
#' @export
#' @importFrom glue glue
#' @importFrom DBI dbExecute
spectrum_add_partition <- function(dbcon, table_name, part_name, part_value, base_location) {
  stopifnot("ident" %in% class(table_name))

  # if the partition already exists, drop it
  table_parts <- table_parts(table_name)
  matching_partitions <- nrow(dbGetQuery(dbcon, glue("select * from SVV_EXTERNAL_PARTITIONS where schemaname = '{table_parts[1]}' and tablename = '{table_parts[2]}' and values ILIKE '%{part_value}%'")))
  if (matching_partitions >= 1) {
    log_if_verbose(glue("Partition already exists, {part_name}='{part_value}'"))
  } else {
    sql_code <- glue("alter table {table_name} add partition({part_name}='{part_value}') location '{base_location}/{part_name}={part_value}'")
    log_if_verbose("Establishing partition: ", sql_code)
    dbExecute(dbcon, sql_code)
  }
  NULL
}

#' Define the DDL for an External Table
#'
#' @param dbcon a database connection
#' @param d data.frame
#' @param table_name Result from dbplyr::in_schema specifying the table and the schema
#' @param location s3:// style url
#' @param partitioned_by character element containing the column name
#'
#' @return Not specified, dbExecute creates table as side effect
#' @export

create_external_table <- function(dbcon, d, table_name, location, partitioned_by = "") {
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
    partitioned_by_spec <- paste0("PARTITIONED BY (", paste0(paste0(redshift_colnames[partitioned_by_index], " ", redshift_types[partitioned_by_index]), collapse = ","), ")", collapse = "")
    redshift_colnames <- redshift_colnames[-partitioned_by_index]
    redshift_types <- redshift_types[-partitioned_by_index]
  }

  warnifnoschema(table_name)
  column_specification <- paste0(paste0(redshift_colnames, " ", redshift_types), collapse = ",")
  dbExecute(dbcon, glue("CREATE EXTERNAL TABLE {table_name} ({column_specification}) {partitioned_by_spec} STORED AS parquet LOCATION '{location}'"))
}
