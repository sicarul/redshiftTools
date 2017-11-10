log_if_verbose <- function(...) {
  if (isTRUE(getOption("redshiftTools.verbose"))) {
    message(...)
  }
}

stopifnoschema <- function(table_name) {
  assertthat::assert_that("ident" %in% class(table_name), msg = "Table name must be result of dbplyr::in_schema()")
}

warnifnoschema <- function(table_name) {
  if (!"ident" %in% class(table_name)) {
    warning(glue("No schema specified for {table_name}, will default to using public"))
    return(FALSE)
  } else {
    return(TRUE)
  }
}

#' Simple coalece
#'
#' A simple non-type aware coalece
#'
#' @aliases coalesceifnull %||%
#' @params x left position
#' @params y right position
coalesceifnull <- function(x, y) {
  return(x %||% y)
}
`%||%` <- function(x, y) if (is.null(x) || is.na(x) || length(x) == 0) y else x


#' @importFrom reticulate import
boto <- reticulate::import("boto", delay_load = TRUE)

bucket_exists <- function(bucket) {
  !is.null(boto$connect_s3()$lookup(bucket))
}

#' Compare data.frame schema (as detected) to db schema
#'
#' @param dbcon A database connection object
#' @param data data.frame
#' @param table character table name
#'
#' @return Side effect of printing result
#' @export
#'
#' @importFrom glue glue
#' @importFrom dplyr left_join
#' @importFrom magrittr %>%
#' @importFrom knitr kable
compare_schema_d_to_db <- function(dbcon, data, table) {
  print(kable(DBI::dbGetQuery(dbcon, glue("select \"column\", type as type_on_db from PG_TABLE_DEF where tablename = '{table_name}'")) %>%
    left_join(data.frame(column = names(data), detected_type = identify_rs_types(data)), by = "column")))
}

# Internal utility functions used by the redshift tools
#'
#' uploadToS3 handles the -test thing on its own since it uses the zapieR methods
#' @param data data.frame
#' @param bucket character
#' @param split_files Number of files to split the data.frame into when uploading to S3
#'
#' @importFrom aws.s3 put_object
#' @importFrom utils write.csv
#' @importFrom zapieR data_science_storage_s3 data_monolith_etl_s3 data_monolith_staging_s3
#' @importFrom data.table fwrite

uploadToS3 <- function(data, bucket, split_files) {
  prefix <- paste0(sample(letters, 32, replace = TRUE), collapse = "")

  # this use of locate_credentials works on a IAM Role provisioned box, it is unclear how it would work in other environments
  if (!bucket_exists(bucket)) {
    stop("Bucket does not exist")
  }

  if (nrow(data) == 0) {
    stop("Input data is empty")
  }

  if (nrow(data) < split_files) {
    split_files <- nrow(data)
  }

  splitted <- suppressWarnings(split(data, seq(1:split_files)))
  # shared state in boto means that this can't go parallel at this time, small files choke it
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
    part <- as.data.frame(lapply(part, function(y) gsub("\\n", "", y)))
    data.table::fwrite(
      part, tmpFile, sep = "|", na = "", col.names = T,
      # ending a line with the delimiter is required, otherwise stl_load_errors
      # will return: Delimiter not found
      eol = "|\n"
    )

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
  file_names <- paste(prefix, ".", formatC(1:split_files, width = 4, format = "d", flag = "0"), ".psv.gz", sep = "")
  s3_bucket <- boto$connect_s3()$get_bucket(bucket)
  s3_bucket$delete_keys(sapply(file_names, s3_bucket$get_key))
  NULL # error message otherwise is TypeError: 'MultiDeleteResult' object is not iterable
}

#' @importFrom DBI dbGetQuery
queryDo <- function(dbcon, query) {
  dbGetQuery(dbcon, query)
}

#' @importFrom assertthat assert_that
#' @importFrom DBI dbGetQuery
#' @importFrom whisker whisker.render
get_table_schema <- function(dbcon, table) {
  assertthat::assert_that(length(table) <= 2)
  assertthat::assert_that("character" %in% class(table))
  if (is.atomic(table)) {
    schema <- "public"
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

#' Fix the order of columns in d to match the underlying Redshift table
#'
#' Internal function for redshiftTools
#'
#' @param d data frame
#' @param dbcon db connection
#' @param table_name character element
#' @param strict boolean
#'
#' @importFrom whisker whisker.render
#' @importFrom DBI dbGetQuery
#' @importFrom dplyr select_ mutate_
#' @importFrom magrittr %>%
#' @importFrom stats setNames
fix_column_order <- function(d, dbcon, table_name, strict = TRUE) {
  if (!"tbl_sql" %in% class(try(dbcon %>% dplyr::tbl(table_name)))) {
    stop(table_name, " does not exist")
  }
  column_names <- get_table_schema(dbcon, table_name)$column

  # catch unsanitized names in redshift...doesn't hurt to sanitize if already sanitized
  column_names <- sanitize_column_names_for_redshift(tolower(column_names))

  # redshift doesn't respect case, but needs some hand holding
  # now we can compare sanitized to sanitized
  names(d) <- sanitize_column_names_for_redshift(tolower(names(d)))

  if (!strict) {
    # add columns to redshift db
    for (missing_column in names(d)[!(names(d) %in% column_names)]) {
      rs_type <- identify_rs_types(d %>% select_(.dots = missing_column))
      rs_add_column(dbcon, table_name = table_name, column_name = missing_column, redshift_type = rs_type)
      column_names <- get_table_schema(dbcon, table_name)$column
    }
    # add columns to data
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

#' Find the number of slices on the cluster
#'
#' We use this as a hint for how many pieces we should slice our data into.
#' We memoise this query for 1 hour because we may end up doing it several times,
#' and the result can't change quickly because Redshift resizes take quite some time.
#'
#' @importFrom memoise memoise timeout
#' @params dbcon A database connection object
number_of_slices <- memoise::memoise(function(dbcon) {
  message("Getting number of slices from Redshift")
  slices <- queryDo(dbcon, "select count(1) from stv_slices")
  unlist(slices[1] * 4)
}, ~timeout(60 * 60))

#' Identify the number of files to generate given a dataset
#'
#' Per Redshift documentation we're aiming for between 1 MB and 1 GB (after compression).
#' Since we know almost nothing about how much compression we'll get, we'll target 100 MB
#' raw sizes with the expectation that we'll compress down to something north of 1 MB and
#' with the fervent hope that we'll upload files in parallel again at some point.
#'
#' @param data data.frame
#' @param dbcon the database connection
#' @importFrom utils object.size
choose_number_of_splits <- function(data, dbcon) {
  mb_target <- 100
  slices <- number_of_slices(dbcon)
  object_size_in_mb <- as.numeric(object.size(data) / 1024 / 1024)
  # check for execessive fragmentation
  if (object_size_in_mb <= slices) {
    split_files <- 1
  } else {
    # If more than slices mb per slice, then open up a new round of slices
    split_files <- (floor(object_size_in_mb / mb_target / slices) + 1) * slices
  }
  # Sanity check: No need for more splits than rows
  if (nrow(data) < split_files) {
    split_files <- 1
  }
  message(sprintf("%s slices detected, will split into %s files", slices, split_files))
  return(split_files)
}

make_dot <- function(table, schema = NULL) {
  assertthat::is.scalar(table)
  if (is.null(schema)) {
    schema <- "public"
  }
  assertthat::is.scalar(schema)
  paste0(schema, ".", table)
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

dot_to_vec <- function(x) {
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
                       secret_key = NULL) {
  gen_credentials <- function(access_key, secret_key) {
    return(glue("credentials 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'"))
  }

  if (!missing(access_key) && !missing(secret_key) && !is.null(access_key) && !is.null(secret_key)) {
    # Uses access_key and secret_key if provided
    return(gen_credentials(access_key, secret_key))
  } else if (nchar(Sys.getenv("REDSHIFT_ROLE")) > 0) {
    # otherwise use REDSHIFT_ROLE env var if available,
    return(glue("IAM_ROLE '{Sys.getenv('REDSHIFT_ROLE')}'"))
  } else if (nchar(Sys.getenv("AWS_ACCESS_KEY_ID")) > 0 && nchar(Sys.getenv("AWS_SECRET_ACCESS_KEY"))) {
    return(gen_credentials(Sys.getenv("AWS_ACCESS_KEY_ID"), Sys.getenv("AWS_SECRET_ACCESS_KEY")))
  } else {
    stop("Unable to generate Redshift credentials; check for AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY env vars or a REDSHIFT_ROLE env var")
  }
}
