# Internal utility functions used by the redshift tools

if(getRversion() >= "2.15.1")  utils::globalVariables(c("i", "obj"))

#' @importFrom "aws.s3" "put_object" "bucket_exists"
#' @importFrom "utils" "write.csv"
#' @importFrom "parallel" "detectCores" "makeCluster" "stopCluster"
#' @importFrom "foreach" "foreach" "%dopar%" "registerDoSEQ"
#' @importFrom "doParallel" "registerDoParallel"
uploadToS3 = function(data, bucket, split_files, key, secret, session, region){
  prefix=paste0(sample(rep(letters, 10),50),collapse = "")
  if(!bucket_exists(bucket, key=key, secret=secret, session=session, region=region)){
    stop("Bucket does not exist")
  }
  splitted = suppressWarnings(split(data, seq(1:split_files)))

  upload_part = function(i){

    return(ifelse(res==TRUE,0,1))
  }

  cores = pmin(detectCores(), 4) # Up to 4 in parallel is fine
  cl = makeCluster(cores)
  registerDoParallel(cl)

  message(paste("Uploading", split_files, "files with prefix", prefix, "to bucket", bucket))

  res = foreach (i=1:split_files, .combine='c') %dopar% {
    part = data.frame(splitted[i])

    tmpFile = tempfile()
    s3Name=paste(bucket, "/", prefix, ".", formatC(i, width = 4, format = "d", flag = "0"), sep="")
    write.csv(part, gzfile(tmpFile, encoding="UTF-8"), na='', row.names=F, quote=T)

    put_object(file = tmpFile, object = s3Name, bucket = "", key=key, secret=secret,
        session=session, region=region)
  }

  stopCluster(cl)
  registerDoSEQ()

  if(length(which(!res)) > 0){
    warning("Error uploading data!")
    return(NA)
  }else{
    message("Upload to S3 complete!")
    return(prefix)
  }
}

#' @importFrom "aws.s3" "delete_object"
deletePrefix = function(prefix, bucket, split_files, key, secret, session, region){
  prev_reg=Sys.getenv('AWS_DEFAULT_REGION')
  Sys.setenv( 'AWS_DEFAULT_REGION'=region)

  s3Names=paste(prefix, ".", formatC(1:split_files, width = 4, format = "d", flag = "0"), sep="")


  cores = pmin(detectCores(), 4) # Up to 4 in parallel is fine
  cl = makeCluster(cores)
  registerDoParallel(cl)

  message(paste("Deleting", split_files, "files with prefix", prefix, "from bucket", bucket))

  res = foreach (obj=s3Names, .combine='c') %dopar% {
    delete_object(obj, bucket, key=key, secret=secret, session=session, region=region)
  }

  stopCluster(cl)
  registerDoSEQ()

  Sys.setenv( 'AWS_DEFAULT_REGION'=prev_reg)
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
