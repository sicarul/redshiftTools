#' Replace or upsert redshift table
#'
#' Upload a table to S3 and then load it with redshift, replacing the contents of that table.
#' The table on redshift has to have the same structure and column ordering to work correctly.
#'
#' @param data a data frame
#' @param dbcon an RPostgres connection to the redshift server
#' @param table_name the name of the table to replace
#' @param split_files optional parameter to specify amount of files to split into. If not specified will look at amount of slices in Redshift to determine an optimal amount.
#' @param bucket the name of the temporary bucket to load the data. Will look for AWS_BUCKET_NAME on environment if not specified.
#' @param region the region of the bucket. Will look for AWS_DEFAULT_REGION on environment if not specified.
#' @param access_key the access key with permissions for the bucket. Will look for AWS_ACCESS_KEY_ID on environment if not specified.
#' @param secret_key the secret key with permissions fot the bucket. Will look for AWS_SECRET_ACCESS_KEY on environment if not specified.
#' @param iam_role_arn an iam role arn with permissions fot the bucket. Will look for AWS_IAM_ROLE_ARN on environment if not specified. This is ignoring access_key and secret_key if set.
#' @param wlm_slots amount of WLM slots to use for this bulk load http://docs.aws.amazon.com/redshift/latest/dg/tutorial-configuring-workload-management.html
#' @examples
#' library(DBI)
#'
#' a=data.frame(a=seq(1,10000), b=seq(10000,1))
#'
#'\dontrun{
#' con <- dbConnect(RPostgres::Postgres(), dbname="dbname",
#' host='my-redshift-url.amazon.com', port='5439',
#' user='myuser', password='mypassword',sslmode='require')
#'
#' rs_replace_table(data=a, dbcon=con, table_name='testTable',
#' bucket="my-bucket", split_files=4)
#'
#' }
#' @export
rs_replace_table = function(
    data,
    dbcon,
    table_name,
    split_files,
    bucket=Sys.getenv('AWS_BUCKET_NAME'),
    region=Sys.getenv('AWS_DEFAULT_REGION'),
    access_key=Sys.getenv('AWS_ACCESS_KEY_ID'),
    secret_key=Sys.getenv('AWS_SECRET_ACCESS_KEY'),
    iam_role_arn=Sys.getenv('AWS_IAM_ROLE_ARN'),
    wlm_slots=1
    )
  {

  Sys.setenv('AWS_DEFAULT_REGION'=region)
  Sys.setenv('AWS_ACCESS_KEY_ID'=access_key)
  Sys.setenv('AWS_SECRET_ACCESS_KEY'=secret_key)
  Sys.setenv('AWS_IAM_ROLE_ARN'=iam_role_arn)

  if(missing(split_files)){
    print("Getting number of slices from Redshift")
    slices = queryDo(dbcon,"select count(*) from stv_slices")
    split_files = unlist(slices[1]*4)
    print(sprintf("%s slices detected, will split into %s files", slices, split_files))
  }
  split_files = min(split_files, nrow(data))

  prefix = uploadToS3(data, bucket, split_files)

  if(wlm_slots>1){
    queryStmt(dbcon,paste0("set wlm_query_slot_count to ", wlm_slots));
  }

  result = tryCatch({
      stageTable=s3ToRedshift(dbcon, table_name, bucket, prefix, region, access_key, secret_key, iam_role_arn)

      # Use a single transaction if using RJDBC
      if(inherits(dbcon, 'RJDBC')){
        queryStmt(dbcon, 'begin')
      }

      print("Deleting target table for replacement")
      queryStmt(dbcon, sprintf("delete from %s", table_name))

      print("Insert new rows")
      queryStmt(dbcon, sprintf('insert into %s select * from %s', table_name, stageTable))

      print("Drop staging table")
      queryStmt(dbcon, sprintf("drop table %s", stageTable))

      print("Committing changes")
      queryStmt(dbcon, "COMMIT;")

      return(TRUE)
  }, warning = function(w) {
      print(w)
  }, error = function(e) {
      print(e$message)
      queryStmt(dbcon, 'ROLLBACK;')
      return(FALSE)
  }, finally = {
    print("Deleting temporary files from S3 bucket")
    deletePrefix(prefix, bucket, split_files)
  })

  return (result)
}
