#' Translate the types of a given data.frame to Redshift types
#'
#' @param .data data.frame, tbl_df
#'
#' @return character
#' @importFrom dplyr recode
#'
#' @examples
#' \dontrun{redshiftTools:::identify_rs_types(mtcars)}
#'
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
  data_types <- recode(
    unlist(classes_first_pass),
    factor = "VARCHAR(255)",
    numeric = "FLOAT8",
    integer = "BIGINT",
    character = "VARCHAR(255)",
    logical = "BOOLEAN")
  return(data_types)
}

# Internal utility functions used by the redshift tools

#' @importFrom aws.s3 put_object bucket_exists
#' @importFrom utils write.csv
#' @importFrom zapieR data_science_storage_s3 data_monolith_etl_s3 data_monolith_staging_s3
uploadToS3 <- function(data, bucket, split_files) {

  prefix = paste0(sample(letters, 32, replace = TRUE), collapse = "")

  if(!bucket_exists(bucket)) {
    stop("Bucket does not exist")
  }

  if(nrow(data) == 0) {
    stop("Input data is empty")
  }

  if(nrow(data) < split_files) {
    split_files <- nrow(data)
  }

  splitted <- suppressWarnings(split(data, seq(1:split_files)))
  #shared state in boto means that this can't go parallel at this time, small files choke it
  lapply(1:split_files, function(i) {

    part <- data.frame(splitted[i])

    tmpFile <- tempfile()
    tmpFile <- paste0(tmpFile, ".psv")
    s3Name <- paste0(paste(prefix, ".", formatC(i, width = 4, format = "d", flag = "0"), sep = ""), ".psv.gz")
    # http://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-format.html
    # The default redshift delimiter is a pipe, "|"
    part <- as.data.frame(lapply(part, function(y) gsub('"', "", y)))
    part <- as.data.frame(lapply(part, function(y) gsub("'", "", y)))
    part <- as.data.frame(lapply(part, function(y) gsub("\\\\", "", y)))
    part <- as.data.frame(lapply(part, function(y) gsub("\\|", "\\\\|", y)))
    part <- as.data.frame(lapply(part, function(y) gsub('\\n', "", y)))
    data.table::fwrite(part, tmpFile, sep = "|", na = "", col.names = T,
                       # ending a line with the delimiter is required, otherwise stl_load_errors
                       # will return: Delimiter not found
                       eol = "|\n")

    system(paste("gzip -f", tmpFile))

    message(paste("Uploading", s3Name))
    s3 <- switch(bucket,
           `zapier-data-science-storage` = data_science_storage_s3(),
           `data-monolith-etl` = data_monolith_etl_s3(),
           `data-monolith-staging` = data_monolith_staging_s3(),
          data_science_storage_s3()
    )
    s3$set_file(object = s3Name, file = paste0(tmpFile, ".gz"))
  })

  return(prefix)
}

#' @importFrom "aws.s3" "delete_object"
#' @importFrom parallel mclapply
deletePrefix <- function(prefix, bucket, split_files){
  parallel::mclapply(1:split_files, function(i) {
    s3Name = paste(prefix, ".", formatC(i, width = 4, format = "d", flag = "0"), ".psv.gz", sep="")
    delete_object(s3Name, bucket)
  })
}

#' @importFrom DBI dbGetQuery
queryDo <- function(dbcon, query){
  dbGetQuery(dbcon, query)
}

#' @importFrom assertthat assert_that
#' @importFrom DBI dbGetQuery
#' @importFrom whisker whisker.render
get_table_schema <- function(dbcon, table) {
  assertthat::assert_that(length(table) <= 2)
  assertthat::assert_that("character" %in% class(table))
  if (is.atomic(table)) {
    schema <- 'public'
    target_table <- table
  } else {
    schema <- table[1]
    target_table <- table[2]
  }
  dbGetQuery(dbcon, whisker.render("SELECT *
  FROM pg_table_def
  WHERE tablename = '{{table_name}}'
  AND schemaname = '{{schema}}'", list(table_name = target_table, schema = schema)))
}

#' @importFrom whisker whisker.render
#' @importFrom DBI dbGetQuery
#' @importFrom dplyr select_
#' @importFrom magrittr %>%
fix_column_order <- function(d, dbcon, table_name, strict = TRUE) {
  if (!DBI::dbExistsTable(dbcon, table_name)) {
    stop(table_name, " does not exist")
  }
  column_names <- get_table_schema(dbcon, table_name)$column
  # we want to ignore the rw_ prefix we add, because we add that dynamically at time of
  column_names <- gsub("rw_", "", column_names, fixed = TRUE)
  #redshift doesn't respect case
  names(d) <- tolower(names(d))
  if (!strict) {
    # add columns to redshift db
    for (missing_column in names(d)[!(names(d) %in% column_names)]) {
      rs_type <- identify_rs_types(d %>% select_(.dots = missing_column))
      rs_add_column(dbcon, table_name = table_name, column_name = missing_column, redshift_type = rs_type)
      column_names <- get_table_schema(dbcon, table_name)$column
    }
    #add columns to data
    for (missing_column in column_names[!(column_names %in% names(d))]) {
      d <- d %>% mutate_(.dots = setNames(list("NA"), missing_column))
    }
  }
  if ((!all(names(d) %in% column_names) || !all(column_names %in% names(d)))) {
    message("Names in d but not in column names: ", paste0(names(d)[!(names(d) %in% column_names)], collapse = ", "))
    message("Names in column_names but not in d: ", paste0(column_names[!(column_names %in% names(d))], collapse = ", "))
    stop("Columns are missing from either redshift or the data")
  }
  d %>% select_(.dots = paste0("`", column_names, "`"))
}

RESERVED_WORDS <- readLines(textConnection("AES128
AES256
ALL
ALLOWOVERWRITE
ANALYSE
ANALYZE
AND
ANY
ARRAY
AS
ASC
AUTHORIZATION
BACKUP
BETWEEN
BINARY
BLANKSASNULL
BOTH
BYTEDICT
BZIP2
CASE
CAST
CHECK
COLLATE
COLUMN
CONSTRAINT
CREATE
CREDENTIALS
CROSS
CURRENT_DATE
CURRENT_TIME
CURRENT_TIMESTAMP
CURRENT_USER
CURRENT_USER_ID
DEFAULT
DEFERRABLE
DEFLATE
DEFRAG
DELTA
DELTA32K
DESC
DISABLE
DISTINCT
DO
ELSE
EMPTYASNULL
ENABLE
ENCODE
ENCRYPT
ENCRYPTION
END
EXCEPT
EXPLICIT
FALSE
FOR
FOREIGN
FREEZE
FROM
FULL
GLOBALDICT256
GLOBALDICT64K
GRANT
GROUP
GZIP
HAVING
IDENTITY
IGNORE
ILIKE
IN
INITIALLY
INNER
INTERSECT
INTO
IS
ISNULL
JOIN
LEADING
LEFT
LIKE
LIMIT
LOCALTIME
LOCALTIMESTAMP
LUN
LUNS
LZO
LZOP
MINUS
MOSTLY13
MOSTLY32
MOSTLY8
NATURAL
NEW
NOT
NOTNULL
NULL
NULLS
OFF
OFFLINE
OFFSET
OID
OLD
ON
ONLY
OPEN
OR
ORDER
OUTER
OVERLAPS
PARALLEL
PARTITION
PERCENT
PERMISSIONS
PLACING
PRIMARY
RAW
READRATIO
RECOVER
REFERENCES
RESPECT
REJECTLOG
RESORT
RESTORE
RIGHT
SELECT
SESSION_USER
SIMILAR
SOME
SYSDATE
SYSTEM
TABLE
TAG
TDES
TEXT255
TEXT32K
THEN
TIMESTAMP
TO
TOP
TRAILING
TRUE
TRUNCATECOLUMNS
UNION
UNIQUE
USER
USING
VERBOSE
WALLET
WHEN
WHERE
WITH
WITHOUT"))

choose_number_of_splits <- function(data, dbcon) {
    message("Getting number of slices from Redshift")
    slices <- queryDo(dbcon,"select count(*) from stv_slices")
    split_files <- unlist(slices[1]*4)
    # check for execessive fragmentation
    rows_per_split <- nrow(data) / (slices[1] * 4)
    if (rows_per_split < 1000) {
      split_files <- slices[1]
      # no need for more splits than there are rows
      if (split_files > nrow(data)) {
        split_files <- 1
      }
    }
    # No need for more splits than files
    message(sprintf("%s slices detected, will split into %s files", slices, split_files))
  return(split_files)
}
