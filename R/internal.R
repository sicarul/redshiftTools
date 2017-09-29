warnifnoschema <- function(table_name) {
  if (!"ident" %in% class(table_name)) {
    # The code isn't ready to handle schemaed tables
    # warning("No schema specified for {table_name} using public")
    return(FALSE)
  } else {
    return(TRUE)
  }
}

#' Simple coalece
#'
#' A simple non-type aware coalece
#'
#' @export coalesceifnull %||%
#' @aliases coalesceifnull %||%
coalesceifnull <- function(x,y) {return(x %||% y)}
`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x


#' @importFrom reticulate import
boto <- reticulate::import("boto", delay_load = TRUE)

bucket_exists <- function(bucket) {
  !is.null(boto$connect_s3()$lookup(bucket))
}

# Internal utility functions used by the redshift tools
#'
#' uploadToS3 handles the -test thing on its own since it uses the zapieR methods
#'
#' @importFrom aws.s3 put_object
#' @importFrom aws.signature locate_credentials
#' @importFrom utils write.csv
#' @importFrom zapieR data_science_storage_s3 data_monolith_etl_s3 data_monolith_staging_s3

uploadToS3 <- function(data, bucket, split_files) {

  prefix = paste0(sample(letters, 32, replace = TRUE), collapse = "")

  # this use of locate_credentials works on a IAM Role provisioned box, it is unclear how it would work in other environments
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
deletePrefix <- function(prefix, bucket, split_files) {
  file_names <- paste(prefix, ".", formatC(1:split_files, width = 4, format = "d", flag = "0"), ".psv.gz", sep="")
  s3_bucket <- boto$connect_s3()$get_bucket(bucket)
  s3_bucket$delete_keys(sapply(file_names, s3_bucket$get_key))
  NULL #error message otherwise is TypeError: 'MultiDeleteResult' object is not iterable
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
  if (!"tbl_sql" %in% class(try(dbcon %>% dplyr::tbl(table_name)))) {
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

make_dot <- function(table, schema = NULL) {
  assertthat::is.scalar(table)
  if (is.null(schema)) {
    schema <- "public"
  }
  assertthat::is.scalar(schema)
  paste0(schema,".",table)
}

vec_to_dot <- function(x) {
  if (length(x) == 1) {
    schema <- "public"
    table <- x
  } else {
    assertthat::assert_that(length(x) == 2)
    schema <- x[1]
    table <- x[2]
  }
  make_dot(table, schema)
}

dot_to_vec  <- function(x) {
  strsplit(x, split = ".", fixed = TRUE)[[1]]
}


#' Make Credentials
#'
#' Uses access_key and secret_key if provided,
#' otherwise use REDSHIFT_ROLE env var if available,
#' otherwise use AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY env var,
#' otherwise error.
#'
#' REDSHIFT_ROLE should be the full resource address, e.g. arn:aws:iam::###:role/{name}
#'
#' @param access_key character (optional)
#' @param secret_key character (optional)
#' @return character string to append to provide creds to Redshift
#'
#' @importFrom glue glue
make_creds <- function(
  access_key = NULL,
  secret_key = NULL
) {
  gen_credentials <- function(access_key, secret_key) {
    return(glue("credentials 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'"))
  }

  if (!missing(access_key) & !missing(secret_key)) {
    # Uses access_key and secret_key if provided
    return(gen_credentials(access_key, secret_key))
  } else if (nchar(Sys.getenv('REDSHIFT_ROLE')) > 0) {
    # otherwise use REDSHIFT_ROLE env var if available,
    return(glue("IAM_ROLE '{Sys.getenv('REDSHIFT_ROLE')}'"))
  } else if (nchar(Sys.getenv('AWS_ACCESS_KEY_ID')) > 0 && nchar(Sys.getenv('AWS_SECRET_ACCESS_KEY'))) {
    return(gen_credentials(Sys.getenv('AWS_ACCESS_KEY_ID'), Sys.getenv('AWS_SECRET_ACCESS_KEY')))
  } else {
    stop("Unable to generate Redshift credentials; check for AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY env vars or a REDSHIFT_ROLE env var")
  }
}
