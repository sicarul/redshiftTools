#' Upsert Redshift table by specifying a column list
#'
#' Upload a table to S3 and then load to Redshift, replacing the target value
#' in existing rows that have the same keys, and inserting rows with new keys.
#' New rows must match structure and column ordering of existing Redshift table.
#' @link{https://docs.aws.amazon.com/redshift/latest/dg/merge-specify-a-column-list.html}
#'
#' @param dat a data frame
#' @param dbcon connection to the Redshift server
#' @param table_name the name of the table to replace
#' @param split_files how many files to split into. Default based on number of slices in Redshift.
#' @param values the columns that reflect updated values
#' @param keys the key columns on which to upsert
#' @param bucket temporary AWS bucket; defaults to AWS_BUCKET_NAME
#' @param region temporary AWS region; defaults to AWS_DEFAULT_REGION
#' @param access_key AWS access key; defaults to AWS_ACCESS_KEY_ID
#' @param secret_key AWS secret key; defaults to AWS_SECRET_ACCESS_KEY
#' @param iam_role_arn IAM role; defaults to AWS_IAM_ROLE_ARN. Ignores access_key and secret_key.
#' @param wlm_slots number of WLM slots for bulk load @link{http://docs.aws.amazon.com/redshift/latest/dg/tutorial-configuring-workload-management.html}
#' @param additional_params Additional paramseters to Redshift COPY statement
#'
#' @examples
#' library(DBI)
#'
#' a=data.frame(a=seq(1,10000), b=seq(10000,1))
#' n=head(a,n=5000)
#' n$b=n$a
#' nx=rbind(n, data.frame(a=seq(99999:104000), b=seq(104000:99999)))
#'
#'\dontrun{
#' con <- dbConnect(RPostgres::Postgres(), dbname="dbname",
#' host='my-redshift-url.amazon.com', port='5439',
#' user='myuser', password='mypassword',sslmode='require')
#'
#' rs_cols_upsert_table(df=nx, dbcon=con, table_name='testTable',
#' bucket="my-bucket", split_files=4, values=c('b'), keys=c('a'))
#'}
#' @export
rs_cols_upsert_table = function(dat,
                                dbcon,
                                table_name,
                                values,
                                keys,
                                split_files,
                                bucket = Sys.getenv('AWS_BUCKET_NAME'),
                                region = Sys.getenv('AWS_DEFAULT_REGION'),
                                access_key = Sys.getenv('AWS_ACCESS_KEY_ID'),
                                secret_key = Sys.getenv('AWS_SECRET_ACCESS_KEY'),
                                iam_role_arn = Sys.getenv('AWS_IAM_ROLE_ARN'),
                                wlm_slots = 1,
                                additional_params = '') {
  if (!inherits(dat, 'data.frame')) {
    warning("dat must be a data.frame or inherit from data.frame")
    return(FALSE)
  }
  numRows = nrow(dat)

  if (numRows == 0) {
    warning("Empty dataset provided, will not try uploading")
    return(FALSE)
  }

  if (getOption("verbose")) {
    message("The provided data.frame has ", numRows, " rows")
  }

  if (missing(split_files)) {
    split_files = splitDetermine(dbcon)
  }
  split_files = pmin(split_files, numRows)

  # Upload data to S3
  prefix = uploadToS3(dat, bucket, split_files, access_key, secret_key, region)

  if (wlm_slots > 1) {
    queryStmt(dbcon, paste0("set wlm_query_slot_count to ", wlm_slots))
  }

  result = tryCatch({
    stageTable = s3ToRedshift(
      dbcon,
      table_name,
      bucket,
      prefix,
      region,
      access_key,
      secret_key,
      iam_role_arn,
      additional_params
    )

    # Use a single transaction
    queryStmt(dbcon, 'begin')

    # values to update
    setValues = paste(values, "=", stageTable, ".", values,
                      sep="", collapse=", ")

    # keys aren't optional
    # where stage.key = table.key and...
    keysWhere = paste(stageTable, ".", keys, "=", table_name, ".", keys,
                      sep="", collapse=" and ")
    qu <- sprintf('UPDATE %s \nSET %s \nFROM %s \nWHERE %s',
                  table_name,
                  setValues,
                  stageTable,
                  keysWhere
    )
    if (getOption("verbose")) {
      message(qu)
    }
    res <- queryStmt(dbcon, qu)
    if (getOption("verbose")) {
      message(res, " rows")
    }

    qu <- sprintf('DELETE FROM %s \nUSING %s \nWHERE %s',
                  stageTable,
                  table_name,
                  keysWhere
    )
    if (getOption("verbose")) {
      message(qu)
    }
    res <- queryStmt(dbcon, qu)
    if (getOption("verbose")) {
      message(res, " rows")
    }

    print("Insert new rows")
    qu <- sprintf('INSERT INTO %s \nSELECT * FROM %s',
                  table_name,
                  # paste(values, collapse=", "),
                  # paste(values, collapse=", "),
                  stageTable)
    if (getOption("verbose")) {
      message(qu)
    }
    res <- queryStmt(dbcon, qu)
    if (getOption("verbose")) {
      message(res, " rows")
    }

    message("Commiting")
    queryStmt(dbcon, "COMMIT;")

    qu <- sprintf("DROP TABLE %s", stageTable)
    message(qu)
    res <- queryStmt(dbcon, qu)
    if (getOption("verbose")) {
      message(res, " rows")
    }

    return(TRUE)
  }, warning = function(w) {
    print(w)
  }, error = function(e) {
    print(e$message)
    queryStmt(dbcon, 'ROLLBACK;')
    return(FALSE)
  }, finally = {
    print("Deleting temporary files from S3 bucket")
    deletePrefix(prefix, bucket, split_files, access_key, secret_key, region)
  })

  return (result)
}
