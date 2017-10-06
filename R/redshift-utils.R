REDSHIFT_RESERVED_WORDS <- readLines(textConnection("AES128
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

#' Make Column Names that Redshift Can Be Happy With
#'
#' @param .data data.frame or character vector
#'
#' @return character vector
#' @export
#' @importFrom glue glue
#' @importFrom assertthat assert_that
sanitize_column_names_for_redshift <- function(.data) {
  if (is.character(.data) & is.vector(.data)) {
    column_names_original <- .data
  } else {
    assert_that("data.frame" %in% class(.data), msg = ".data in sanitize_column_names_for_redshift should be data.frame or a character vector")
    column_names_original <- names(.data)
  }

  column_names <- column_names_original
  column_name_is_reserved <- tolower(column_names) %in% tolower(REDSHIFT_RESERVED_WORDS)
  column_names[column_name_is_reserved] <- paste0('rw_', column_names[column_name_is_reserved])
  if (length(column_name_is_reserved) > 0) {
    message(glue("replacing column name '{column_names_original[column_name_is_reserved]}' with '{column_names[column_name_is_reserved]}' because the original is a reserved name in Redshift.  "))
  }
  column_name_contains_period <- grepl(".", column_names, fixed=TRUE)
  column_names <- gsub(".", "_", column_names, fixed = TRUE)
  if (length(column_name_is_reserved) > 0) {
    message(glue("replacing column name '{column_names_original[column_name_contains_period]}' with '{column_names[column_name_contains_period]}' because periods perform poorly as column names in Redshift.  "))
  }
  return(column_names)
}

#' Identify Corresponding Redshift Types
#'
#' @param .data \code{data.frame}
#' @param character_length The length you want for your VARCHAR, by default we'll just use 10% more than the max value we see in the provided data.
#'
#' @return
#' @export
#'
#' @importFrom glue glue
#' @importFrom dplyr coalesce recode
identify_rs_types <- function (.data, character_length = NA_real_)
{
  classes <- lapply(.data, class)
  classes_first_pass <- lapply(classes, function(x) {
    if (all(c("POSIXct", "POSIXt") %in% x)) {
      x <- "TIMESTAMP"
    }
    return(x)
  })
  max_char_length <- .data %>%
    select(which(classes_first_pass == "character")) %>%
    map(~ max(nchar(.x %||% "") %||% 0)) %>%
    unlist %>%
    as.numeric %>%
    {. %||% 0} %>%
    max
  max_factor_length <- .data %>%
    select(which(classes_first_pass == "factor")) %>%
    map(~ max(nchar(levels(.x)) %||% 0)) %>%
    unlist %>%
    as.numeric %>%
    {. %||% 0} %>%
    max
  varchar_length <- coalesce(character_length, ceiling(max(c(max_char_length, max_factor_length), na.rm = TRUE) * 1.1))
  if (any("factor" %in% classes_first_pass)) {
    warning("one of the columns is a factor")
  }
  data_types <- recode(unlist(classes_first_pass), factor = as.character(glue("VARCHAR({varchar_length})")),
                       numeric = "FLOAT8", integer = "INT", integer64 = "BIGINT", character = as.character(glue("VARCHAR({varchar_length})")),
                       logical = "BOOLEAN", Date = "DATE")
  return(data_types)
}

#' Find recent redshift errors
#'
#' Newest errors at the top
#'
#' @param con Redshift connection
#' @param n Number of errors to show
#'
#' @return data.frame
#' @export
#' @importFrom whisker whisker.render
#' @importFrom DBI dbGetQuery
#' @importFrom stringr str_trim
#' @importFrom dplyr arrange desc select collect
#' @importFrom purrr map
recent_errors <- function(con, n = 10) {
  dbGetQuery(con, whisker.render("select colname, type, raw_field_value, err_reason, starttime from stl_load_errors limit {{limit}}",
        list(
          limit = n
        )
  )) %>% arrange(desc(starttime)) %>%
    select(-starttime) %>%
    collect(n = Inf) %>%
    purrr::map(stringr::str_trim) %>%
    as.data.frame()
}


#' Show definition for a view
#'
#' @param dbcon A database connection object
#' @param view_name The view you'd like to describe
#'
#' @return character
#' @export
#'
#' @examples
#' @importFrom whisker whisker.render
#' @importFrom DBI dbGetQuery
view_definition <- function(dbcon, view_name) {
  dbGetQuery(dbcon, whisker.render("select view_definition from information_schema.views where table_name = '{{view_name}}'", list(view_name = view_name)))
}

#' Check if table exists
#'
#' Passing this check does not mean that the user has access to the table per se.
#'
#' @param dbcon database connection
#' @param table_name dbplyr::in_schema result
#'
#' @return boolean
#' @export
#' @importFrom zapieR whisker.render.recursive
#' @importFrom dplyr pull
rs_table_exists <- function(dbcon, table_name) {
  stopifnoschema(table_name)
  # Many don't respect in_schema, but elsewhere in our code we'd like to pass it around.  So, hack around the problem.
  split_res <- strsplit(table_name, ".", fixed = TRUE)[[1]]
  schemaname <- split_res[1]
  tablename <- split_res[2]
  # Two places to look, first check if this schema is external
  select_exists <- "SELECT EXISTS ( {{query}} ) as present"
  check_external_schema_exists <- "select 1 from SVV_EXTERNAL_SCHEMAS where schemaname = '{{schemaname}}'"
  check_external_schema <- "SELECT 1 from SVV_EXTERNAL_TABLES where schemaname = '{{schemaname}}' and tablename = '{{tablename}}'"
  check_internal_schema <- "SELECT 1 from SVV_TABLES where table_schema = '{{schemaname}}' and table_name = '{{tablename}}'"

  schema_is_external <- dbGetQuery(
    dbcon,
    whisker.render.recursive(select_exists, list(query = check_external_schema_exists, schemaname = schemaname))
  )  %>%
    pull(present)

  check_method <- ifelse(schema_is_external, check_external_schema, check_internal_schema)

  return(
    dbGetQuery(
      dbcon,
      whisker.render.recursive(select_exists, list(query = check_method, schemaname = schemaname, tablename = tablename))
    )  %>%
      pull(present)
  )
}
