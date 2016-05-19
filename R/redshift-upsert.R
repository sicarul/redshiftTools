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
#' rs_upsert_table(data=nx, dbcon=con, tableName='testTable',
#' bucket="my-bucket", split_files=4, keys=c('a'))
#'
#'}
#' @export
rs_upsert_table = function(
    data,
    dbcon,
    tableName,
    keys,
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
    stageTable=paste0(sample(letters,16),collapse = "")

    queryDo(dbcon, sprintf("create temp table %s (like %s)", stageTable, tableName))

    print("Copying data from S3 into Redshift")
    queryDo(dbcon, sprintf("copy %s from 's3://%s/%s.' region '%s' csv gzip ignoreheader 1 emptyasnull credentials 'aws_access_key_id=%s;aws_secret_access_key=%s';",
                        stageTable,
                        bucket,
                        prefix,
                        region,
                        access_key,
                        secret_key
            ))
    if(!missing(keys)){
      print("Deleting rows with same keys")
      keysCond = paste(stageTable,".",keys, "=", tableName,".",keys, sep="")
      keysWhere = sub(" and $", "", paste0(keysCond, collapse="", sep=" and "))
      queryDo(dbcon, sprintf('delete from %s using %s where %s',
              tableName,
              stageTable,
              keysWhere
              ))
    }
    print("Insert new rows")
    queryDo(dbcon, sprintf('insert into %s select * from %s', tableName, stageTable))

    queryDo(dbcon, sprintf("drop table %s", stageTable))
    print("Commiting")
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
