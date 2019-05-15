#' Upsert Redshift table by specifying a column list
#'
#' Upload a table to S3 and then load to Redshift, replacing the target value
#' in existing rows that have the same keys, and inserting rows with new keys.
#' New rows must match structure and column ordering of existing Redshift table.
#'
#' @param dat a data frame
#' @param dbcon an RPostgres/RJDBC connection to the redshift server
#' @param table_name the name of the table to update/insert
#' @param split_files optional parameter to specify amount of files to split into. If not specified will look at amount of slices in Redshift to determine an optimal amount.
#' @param values the columns that will be updated
#' @param keys this optional vector contains the variables by which to upsert. If not defined, the upsert becomes an append.
#' @param bucket the name of the temporary bucket to load the data. Will look for AWS_BUCKET_NAME on environment if not specified.
#' @param region the region of the bucket. Will look for AWS_DEFAULT_REGION on environment if not specified.
#' @param access_key the access key with permissions for the bucket. Will look for AWS_ACCESS_KEY_ID on environment if not specified.
#' @param secret_key the secret key with permissions for the bucket. Will look for AWS_SECRET_ACCESS_KEY on environment if not specified.
#' @param session_token the session key with permissions for the bucket, this will be used instead of the access/secret keys if specified. Will look for AWS_SESSION_TOKEN on environment if not specified.
#' @param iam_role_arn an iam role arn with permissions fot the bucket. Will look for AWS_IAM_ROLE_ARN on environment if not specified. This is ignoring access_key and secret_key if set.
#' @param wlm_slots amount of WLM slots to use for this bulk load http://docs.aws.amazon.com/redshift/latest/dg/tutorial-configuring-workload-management.html
#' @param additional_params Additional params to send to the COPY statement in Redshift
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
#' @seealso \url{https://docs.aws.amazon.com/redshift/latest/dg/merge-specify-a-column-list.html}
#' @seealso \url{http://docs.aws.amazon.com/redshift/latest/dg/tutorial-configuring-workload-management.html}
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
                                session_token=Sys.getenv('AWS_SESSION_TOKEN'),
                                iam_role_arn = Sys.getenv('AWS_IAM_ROLE_ARN'),
                                wlm_slots = 1,
                                additional_params = '') {

  message('Initiating Redshift table upsert for table ',table_name)

  if (!inherits(dat, 'data.frame')) {
    warning("dat must be a data.frame or inherit from data.frame")
    return(FALSE)
  }
  numRows = nrow(dat)

  if (numRows == 0) {
    warning("Empty dataset provided, will not try uploading")
    return(FALSE)
  }

  message("The provided data.frame has ", numRows, " rows")

  if (missing(split_files)) {
    split_files = splitDetermine(dbcon)
  }
  split_files = pmin(split_files, numRows)

  # Upload data to S3
  prefix = uploadToS3(dat, bucket, split_files, access_key, secret_key, session_token, region)

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
      session_token,
      iam_role_arn,
      additional_params
    )

    # Use a single transaction
    queryStmt(dbcon, 'begin')

    # values to update
    setValues = paste(values, "=", stageTable, ".", values,
                      sep="", collapse=", ")
    # check that the values actually differ from existing ones
    changedValues = paste(table_name, ".", values, "!=",
                          stageTable, ".", values,
                          sep="", collapse=" OR ")

    # keys aren't optional
    # where stage.key = table.key and...
    keysWhere = paste(stageTable, ".", keys, "=", table_name, ".", keys,
                       sep="", collapse=" and ")
    qu <- sprintf('UPDATE %s \nSET %s \nFROM %s \nWHERE %s \nAND (%s)',
                  table_name,
                  setValues,
                  stageTable,
                  keysWhere,
                  changedValues
    )
    message(qu)
    res <- queryStmt(dbcon, qu)
    message(res, " rows affected")

    qu <- sprintf('DELETE FROM %s \nUSING %s \nWHERE %s',
                  stageTable,
                  table_name,
                  keysWhere
    )
    message(qu)
    res <- queryStmt(dbcon, qu)
    message(res, " rows affected")

    message("Insert new rows")
    qu <- sprintf('INSERT INTO %s \nSELECT * FROM %s',
                  table_name,
                  stageTable)
    message(qu)
    res <- queryStmt(dbcon, qu)
    message(res, " rows affected")

    message("Commiting")
    queryStmt(dbcon, "COMMIT;")

    qu <- sprintf("DROP TABLE %s", stageTable)
    message(qu)
    res <- queryStmt(dbcon, qu)
    message(res, " rows affected")

    return(TRUE)
  }, warning = function(w) {
    warning(w)
  }, error = function(e) {
    warning(e$message)
    queryStmt(dbcon, 'ROLLBACK;')
    return(FALSE)
  }, finally = {
    message("Deleting temporary files from S3 bucket")
    deletePrefix(prefix, bucket, split_files, access_key, secret_key, session_token, region)
  })

  return (result)
}
