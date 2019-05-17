# Internal utility functions used by the redshift tools

if(getRversion() >= "2.15.1")  utils::globalVariables(c("i", "obj"))

#' @importFrom "aws.s3" "put_object" "bucket_exists"
#' @importFrom "utils" "write.csv"
#' @importFrom "purrr" "map2"
#' @importFrom "progress" "progress_bar"
uploadToS3 = function(data, bucket, split_files, key, secret, session, region){
  prefix=paste0(sample(rep(letters, 10),50),collapse = "")
  if(!bucket_exists(bucket, key=key, secret=secret, session=session, region=region)){
    stop("Bucket does not exist")
  }

  splitted = suppressWarnings(split(data, seq(1:split_files)))

  message(paste("Uploading", split_files, "files with prefix", prefix, "to bucket", bucket))


  pb <- progress_bar$new(total = split_files, format='Uploading file :current/:total [:bar]')
  pb$tick(0)

  upload_part = function(part, i){
    tmpFile = tempfile()
    s3Name=paste(bucket, "/", prefix, ".", formatC(i, width = 4, format = "d", flag = "0"), sep="")
    write.csv(part, gzfile(tmpFile, encoding="UTF-8"), na='', row.names=F, quote=T)

    r=put_object(file = tmpFile, object = s3Name, bucket = "", key=key, secret=secret,
        session=session, region=region)
    pb$tick()
    return(r)
  }

  res = map2 (splitted, 1:split_files, upload_part)

  if(length(which(!unlist(res))) > 0){
    warning("Error uploading data!")
    return(NA)
  }else{
    message("Upload to S3 complete!")
    return(prefix)
  }
}

#' @importFrom "aws.s3" "delete_object"
#' @importFrom "purrr" "map"
deletePrefix = function(prefix, bucket, split_files, key, secret, session, region){

  s3Names=paste(prefix, ".", formatC(1:split_files, width = 4, format = "d", flag = "0"), sep="")

  message(paste("Deleting", split_files, "files with prefix", prefix, "from bucket", bucket))

  pb <- progress_bar$new(total = split_files, format='Deleting file :current/:total [:bar]')
  pb$tick(0)

  deleteObj = function(obj){
    delete_object(obj, bucket, key=key, secret=secret, session=session, region=region)
    pb$tick()
  }

  res = map(s3Names, deleteObj)
}

#' @importFrom DBI dbGetQuery
queryDo = function(dbcon, query){
  dbGetQuery(dbcon, query)
}

#' @importFrom DBI dbExecute
queryStmt = function(dbcon, query){
  if(inherits(dbcon, 'JDBCConnection')){
    RJDBC::dbSendUpdate(dbcon, query)
  }else{
    dbExecute(dbcon, query)
  }
}

splitDetermine = function(dbcon){
  message("Getting number of slices from Redshift")
  slices = queryDo(dbcon,"select count(*) from stv_slices")
  slices[1] = round(slices[1])
  if(slices[1] < 16){ # Use more if low number of slices
    split_files = 16
  }else{
    split_files = unlist(slices[1])
  }
  message(sprintf("%s slices detected, will split into %s files", slices, split_files))
  return(split_files)
}


s3ToRedshift = function(dbcon, table_name, bucket, prefix, region, access_key, secret_key, session, iam_role_arn, additional_params){
    stageTable=paste0(sample(letters,16),collapse = "")
    # Create temporary table for staging data
    queryStmt(dbcon, sprintf("create temp table %s (like %s)", stageTable, table_name))
    copyStr = "copy %s from 's3://%s/%s.' region '%s' csv gzip ignoreheader 1 emptyasnull COMPUPDATE FALSE STATUPDATE FALSE %s %s"
    # Use IAM Role if available
    if (nchar(iam_role_arn) > 0) {
      credsStr = sprintf("iam_role '%s'", iam_role_arn)
    } else {
      # creds string now includes a token in case it is needed.
      credsStr = sprintf("credentials 'aws_access_key_id=%s;aws_secret_access_key=%s;token=%s'", access_key, secret_key, session)
    }
    statement = sprintf(copyStr, stageTable, bucket, prefix, region, additional_params, credsStr)
    queryStmt(dbcon, statement)

    return(stageTable)
}
