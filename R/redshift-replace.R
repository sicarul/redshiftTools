#' Replace or upsert redshift table
#'
#' Upload a table to S3 and then load it with redshift, replacing the contents of that table.
#' The table on redshift has to have the same structure and column ordering to work correctly.
#'
#' @param data a data frame
#' @param dbcon an RPostgres connection to the redshift server
#' @param tableName the name of the table to replace
#' @param split_files optional parameter to specify amount of files to split into. If not specified will look at amount of slices in Redshift to determine an optimal amount.
#' @param bucket the name of the temporary bucket to load the data. Will look for AWS_BUCKET_NAME on environment if not specified.
#' @param region the region of the bucket. Will look for AWS_DEFAULT_REGION on environment if not specified.
#' @param access_key the access key with permissions for the bucket. Will look for AWS_ACCESS_KEY_ID on environment if not specified.
#' @param secret_key the secret key with permissions fot the bucket. Will look for AWS_SECRET_ACCESS_KEY on environment if not specified.
#' @examples
#' library(DBI)
#'
#' a=data.frame(a=seq(1,10000), b=seq(10000,1))
#'
#' \dontrun{
#' con <- dbConnect(RPostgres::Postgres(), dbname="dbname",
#' host='my-redshift-url.amazon.com', port='5439',
#' user='myuser', password='mypassword',sslmode='require')
#'
#' rs_replace_table(data=a, dbcon=con, tableName='testTable',
#' bucket="my-bucket", split_files=4)
#'
#' }
#' @export
#' @importFrom DBI dbExecute
rs_replace_table <- function(
                             data,
                             dbcon,
                             tableName,
                             split_files,
                             bucket = Sys.getenv("AWS_BUCKET_NAME"),
                             region = Sys.getenv("AWS_DEFAULT_REGION"),
                             # the internal make_creds() handles the defaults for these params
                             access_key = NULL,
                             secret_key = NULL,
                             remove_quotes = TRUE,
                             strict = TRUE,
                             use_transaction = TRUE) {
  if (missing(split_files)) {
    split_files <- choose_number_of_splits(data, dbcon)
  }

  # this functon is only intended in the processs of control flow
  # the occurs immediately after. This function does pretty much
  # all the work. it's not a pure function!
  replace <- function(data, dbcon) {
    split_files <- min(split_files, nrow(data))
    data <- fix_column_order(data, dbcon, table_name = tableName, strict = strict)
    prefix <- uploadToS3(data, bucket, split_files)
    raw_bucket <- paste0(bucket, if (Sys.getenv("ENVIRONMENT") == "production") "" else "-test")
    on.exit({
      message("Deleting temporary files from S3 bucket")
      deletePrefix(prefix, raw_bucket, split_files)
    })
    message("Truncating target table")
    queryDo(dbcon, sprintf("truncate table %s", tableName))
    if (remove_quotes) {
      query_string <- "copy %s from 's3://%s/%s.' region '%s' truncatecolumns acceptinvchars as '^' escape delimiter '|' removequotes gzip ignoreheader 1 emptyasnull STATUPDATE ON COMPUPDATE ON %s;"
    } else {
      query_string <- "copy %s from 's3://%s/%s.' region '%s' truncatecolumns acceptinvchars as '^' escape delimiter '|' gzip ignoreheader 1 emptyasnull STATUPDATE ON COMPUPDATE ON %s;"
    }
    DBI::dbExecute(dbcon, sprintf(
      query_string,
      tableName,
      raw_bucket,
      prefix,
      region,
      make_creds()
    ))
  }

  if (use_transaction) {
    transaction(
      .data = data,
      .dbcon = dbcon,
      list(function(...) {
        replace(data, dbcon)
      })
    )
  } else {
    replace(data, dbcon)
  }
}
