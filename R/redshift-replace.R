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
#'\dontrun{
#' con <- dbConnect(RPostgres::Postgres(), dbname="dbname",
#' host='my-redshift-url.amazon.com', port='5439',
#' user='myuser', password='mypassword',sslmode='require')
#'
#' rs_replace_table(data=a, dbcon=con, tableName='testTable',
#' bucket="my-bucket", split_files=4)
#'
#' }
#' @export
rs_replace_table = function(
    data,
    dbcon,
    tableName,
    split_files,
    bucket=Sys.getenv('AWS_BUCKET_NAME'),
    region=Sys.getenv('AWS_DEFAULT_REGION'),
    access_key=Sys.getenv('AWS_ACCESS_KEY_ID'),
    secret_key=Sys.getenv('AWS_SECRET_ACCESS_KEY')
    )
  {

  Sys.setenv('AWS_DEFAULT_REGION'=region)
  Sys.setenv('AWS_ACCESS_KEY_ID'=access_key)
  Sys.setenv('AWS_SECRET_ACCESS_KEY'=secret_key)

  if(missing(split_files)){
    print("Getting number of slices from Redshift")
    slices = queryDo(dbcon,"select count(*) from stv_slices")
    split_files = unlist(slices[1]*4)
    print(sprintf("%s slices detected, will split into %s files", slices, split_files))
  }
  split_files = min(split_files, nrow(data))

  prefix = uploadToS3(data, bucket, split_files)



  result = tryCatch({
      print("Deleting target table for replacement")
      queryDo(dbcon, sprintf("delete from %s", tableName))

      print("Copying data from S3 into Redshift")
      queryDo(dbcon, sprintf("copy %s from 's3://%s/%s.' region '%s' csv gzip ignoreheader 1 emptyasnull credentials 'aws_access_key_id=%s;aws_secret_access_key=%s';",
                          tableName,
                          bucket,
                          prefix,
                          region,
                          access_key,
                          secret_key
              ))

      print("Committing changes")
      queryDo(dbcon, "COMMIT;")
      return(TRUE)
  }, warning = function(w) {
      print(w)
  }, error = function(e) {
      print(e$message)
      queryDo(dbcon, 'ROLLBACK;')
      return(FALSE)
  }, finally = {
    print("Deleting temporary files from S3 bucket")
    deletePrefix(prefix, bucket, split_files)
  })

  return (result)
}

