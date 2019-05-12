#' Upsert redshift table
#'
#' Upload a table to S3 and then load it with redshift, replacing rows with the same
#' keys, and inserting rows with new keys.
#' The table on redshift has to have the same structure and column ordering to work correctly.
#'
#' @param df a data frame
#' @param dbcon an RPostgres/RJDBC connection to the redshift server
#' @param table_name the name of the table to update/insert
#' @param split_files optional parameter to specify amount of files to split into. If not specified will look at amount of slices in Redshift to determine an optimal amount.
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
#' rs_upsert_table(df=nx, dbcon=con, table_name='testTable',
#' bucket="my-bucket", split_files=4, keys=c('a'))
#'
#'}
#' @export
rs_upsert_table = function(
    df,
    dbcon,
    table_name,
    keys,
    split_files,
    bucket=Sys.getenv('AWS_BUCKET_NAME'),
    region=Sys.getenv('AWS_DEFAULT_REGION'),
    access_key=Sys.getenv('AWS_ACCESS_KEY_ID'),
    secret_key=Sys.getenv('AWS_SECRET_ACCESS_KEY'),
    session_token=Sys.getenv('AWS_SESSION_TOKEN'),
    iam_role_arn=Sys.getenv('AWS_IAM_ROLE_ARN'),
    wlm_slots=1,
    additional_params=''
    )
  {

  message('Initiating Redshift table upsert for table ',table_name)

  if(!inherits(df, 'data.frame')){
    warning("The df parameter must be a data.frame or an object compatible with it's interface")
    return(FALSE)
  }
  numRows = nrow(df)

  if(numRows == 0){
    warning("Empty dataset provided, will not try uploading")
    return(FALSE)
  }

  message(paste0("The provided data.frame has ", numRows, ' rows'))

  if(missing(split_files)){
    split_files = splitDetermine(dbcon)
  }
  split_files = pmin(split_files, numRows)

  # Upload data to S3
  prefix = uploadToS3(df, bucket, split_files, access_key, secret_key, session_token, region)

  if(wlm_slots>1){
    queryStmt(dbcon,paste0("set wlm_query_slot_count to ", wlm_slots));
  }

  result = tryCatch({
    stageTable=s3ToRedshift(dbcon, table_name, bucket, prefix, region, access_key, secret_key, session_token, iam_role_arn, additional_params)

    # Use a single transaction
    queryStmt(dbcon, 'begin')


    if(!missing(keys)){
      # where stage.key = table.key and...
      keysCond = paste(stageTable,".",keys, "=", table_name,".",keys, sep="")
      keysWhere = sub(" and $", "", paste0(keysCond, collapse="", sep=" and "))

      queryStmt(dbcon, sprintf('delete from %s using %s where %s',
              table_name,
              stageTable,
              keysWhere
              ))
    }

    message("Insert new rows")
    queryStmt(dbcon, sprintf('insert into %s select * from %s', table_name, stageTable))

    message("Drop staging table")
    queryStmt(dbcon, sprintf("drop table %s", stageTable))

    message("Commiting")
    queryStmt(dbcon, "COMMIT;")

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
