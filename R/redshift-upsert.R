#' Upsert redshift table
#'
#' Upload a table to S3 and then load it with redshift, replacing rows with the same
#' keys, and inserting rows with new keys.
#' The table on redshift has to have the same structure and column ordering to work correctly.
#'
#' @param data a data frame
#' @param dbcon an RPostgres connection to the redshift server
#' @param tableName the name of the table to replace
#' @param split_files optional parameter to specify amount of files to split into. If not specified will look at amount of slices in Redshift to determine an optimal amount.
#' @param keys athis optional vector contains the variables by which to upsert. If not defined, the upsert becomes an append.
#' @param bucket the name of the temporary bucket to load the data. Will look for AWS_BUCKET_NAME on environment if not specified.
#' @param region the region of the bucket. Will look for AWS_DEFAULT_REGION on environment if not specified.
#' @param access_key the access key with permissions for the bucket. Will look for AWS_ACCESS_KEY_ID on environment if not specified.
#' @param secret_key the secret key with permissions fot the bucket. Will look for AWS_SECRET_ACCESS_KEY on environment if not specified.
#'
#' @importFrom DBI dbGetQuery
#' @importFrom DBI dbExecute
#'
#' @examples
#' library(DBI)
#'
#' a=data.frame(a=seq(1,10000), b=seq(10000,1))
#' n=head(a,n=5000)
#' n$b=n$a
#' nx=rbind(n, data.frame(a=seq(99999:104000), b=seq(104000:99999)))
#'
#' \dontrun{
#' con <- dbConnect(RPostgres::Postgres(), dbname="dbname",
#' host='my-redshift-url.amazon.com', port='5439',
#' user='myuser', password='mypassword',sslmode='require')
#'
#' rs_upsert_table(data=nx, dbcon=con, tableName='testTable',
#' bucket="my-bucket", split_files=4, keys=c('a'))
#'
#' }
#' @export
rs_upsert_table <- function(
                            data,
                            dbcon,
                            tableName,
                            keys = NULL,
                            split_files,
                            bucket=Sys.getenv("AWS_BUCKET_NAME"),
                            region=Sys.getenv("AWS_DEFAULT_REGION"),
                            access_key=NULL,
                            secret_key=NULL,
                            strict = FALSE,
                            use_transaction = TRUE) {
  if (missing(bucket)) {
    stop("Bucket name not specified")
  }

  if (missing(split_files)) {
    split_files <- choose_number_of_splits(data, dbcon)
  }
  # we make the creds as a test before we enter transaction land.
  make_creds(access_key, secret_key)

  # this functon is only intended in the processs of control flow
  # the occurs immediately after. This function does pretty much
  # all the work. it's not a pure function!
  upsert <- function(data, dbcon, keys) {
    tryCatch(
      {
        raw_bucket <- paste0(bucket, if (Sys.getenv("ENVIRONMENT") == "production") "" else "-test")
        split_files <- min(split_files, nrow(data))

        data <- fix_column_order(data, dbcon, table_name = tableName, strict = strict)
        prefix <- uploadToS3(data, bucket, split_files)
        on.exit({
          message("Deleting temporary files from S3 bucket")
          deletePrefix(prefix, raw_bucket, split_files)
        })
        stageTable <- paste0(sample(letters, 32, replace = TRUE), collapse = "")

        DBI::dbExecute(dbcon, sprintf("create temp table %s (like %s)", stageTable, tableName))

        message("Copying data from S3 into Redshift")
        DBI::dbExecute(dbcon, sprintf(
          "copy %s from 's3://%s/%s.' region '%s' truncatecolumns acceptinvchars as '^' escape delimiter '|' removequotes gzip ignoreheader 1 emptyasnull STATUPDATE ON COMPUPDATE ON %s;",
          stageTable,
          raw_bucket,
          prefix,
          region,
          make_creds()
        ))

        if (!is.null(keys)) {
          message("Deleting rows with same keys")
          keysCond <- paste(stageTable, ".", keys, "=", tableName, ".", keys, sep = "")
          keysWhere <- sub(" and $", "", paste0(keysCond, collapse = "", sep = " and "))
          DBI::dbExecute(dbcon, sprintf(
            "delete from %s using %s where %s;",
            tableName,
            stageTable,
            keysWhere
          ))
        }

        message("Insert new rows")
        DBI::dbExecute(dbcon, sprintf("insert into %s (select * from %s);", tableName, stageTable))
        DBI::dbExecute(dbcon, sprintf("drop table %s;", stageTable))
      },
      error = function(e) {
        warning(paste0("Error detected, bubling up", e))
        if (exists("stageTable")) {
          message("Outputing schemas to compare...")
          compare_schema_d_to_db(data, stageTable)
        }
        stop(e)
      }
    )
  }


  if (use_transaction) {
    transaction(
      .data = data,
      .dbcon = dbcon,
      list(function(...) {
        upsert(data, dbcon, keys)
      })
    )
  } else {
    upsert(data, dbcon, keys)
  }
}
