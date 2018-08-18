# Internal utility functions used by the redshift tools

#' @importFrom "aws.s3" "put_object" "bucket_exists"
#' @importFrom "utils" "write.csv"
uploadToS3 = function (data, bucket, split_files, key, secret, region){
  start_time = Sys.time()
  prefix=paste0(sample(rep(letters, 10),50),collapse = "")
  if(!bucket_exists(bucket, key=key, secret=secret, region=region)){
    stop("Bucket does not exist")
  }
  data = mutate_if(data,is.factor,as.character)
  data = mutate_if(data,is.character,enc2utf8)

  toSave = suppressWarnings(split(data, seq(1:split_files)))
  print("Generating CSV...")
  toSave = lapply(1:split_files, function(i){
    list(tmpFile = tempfile(),
         gzFile = tempfile(),
         split = toSave[[i]],
         s3Name = paste(bucket, "/", prefix, ".", formatC(i, width = 4, format = "d", flag = "0"), sep=""))
  })
  setDTthreads(0)
  void = lapply(toSave, function(saved){
    fwrite(saved[["split"]], saved[["tmpFile"]], na='', row.names=F, quote=T)
  })
  print("Uploading data...")
  void = future_lapply(toSave, function(saved){
    print(paste("Uploading", saved[["tmpFile"]], "to" ,saved[["s3Name"]]))
    gzip(saved[["tmpFile"]],destname=saved[["gzFile"]])
    put_object(file = saved[["gzFile"]], object = saved[["s3Name"]], bucket = "", key=key, secret=secret, region=region)
    file.remove(saved[["tmpFile"]],destname=saved[["gzFile"]])
    return(saved[["s3Name"]])
  })
  end_time = Sys.time()
  print(paste("Data uploaded to S3 in",end_time - start_time))
  return(prefix)
}

#' @importFrom "aws.s3" "delete_object"
deletePrefix = function(prefix, bucket, split_files, key, secret, region){
  prev_reg=Sys.getenv('AWS_DEFAULT_REGION')
  Sys.setenv( 'AWS_DEFAULT_REGION'=region)
  for (i in 1:split_files) {
    s3Name=paste(prefix, ".", formatC(i, width = 4, format = "d", flag = "0"), sep="")
    print(paste("Deleting", s3Name))
    delete_object(s3Name, bucket, key=key, secret=secret, region=region)
  }
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
  print("Getting number of slices from Redshift")
  slices = queryDo(dbcon,"select count(*) from stv_slices")
  slices[1] = round(slices[1])
  if(slices[1] < 16){ # Use more if low number of slices
    split_files = 16
  }else{
    split_files = unlist(slices[1])
  }
  print(sprintf("%s slices detected, will split into %s files", slices, split_files))
  return(split_files)
}


s3ToRedshift = function(dbcon, table_name, bucket, prefix, region, access_key, secret_key, iam_role_arn){
    stageTable=paste0(sample(letters,16),collapse = "")
    # Create temporary table for staging data
    queryStmt(dbcon, sprintf("create temp table %s (like %s)", stageTable, table_name))

    print("Copying data from S3 into Redshift")
    # copyStr = "copy %s from 's3://%s/%s.' region '%s' csv ignoreheader 1 emptyasnull COMPUPDATE FALSE %s"
    copyStr = "copy %s from 's3://%s/%s.' region '%s' csv gzip ignoreheader 1 emptyasnull COMPUPDATE FALSE %s"

    # Use IAM Role if available
    if (nchar(iam_role_arn) > 0) {
      credsStr = sprintf("iam_role '%s'", iam_role_arn)
    } else {
      credsStr = sprintf("credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'", access_key, secret_key)
    }
    statement = sprintf(copyStr, stageTable, bucket, prefix, region, credsStr)
    queryStmt(dbcon,statement)

    return(stageTable)
}
