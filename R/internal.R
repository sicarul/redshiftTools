# Internal utility functions used by the redshift tools

if(getRversion() >= "2.15.1")  utils::globalVariables(c("i", "obj"))

#' @importFrom "paws" "s3"
#' @importFrom "readr" "format_csv"
#' @importFrom "purrr" "map2"
#' @importFrom "progress" "progress_bar"
uploadToS3 = function(data, bucket, split_files, key, secret, session, region){

  prefix = paste0(sample(rep(letters, 10),50), collapse = "")

  if(!bucket_exists(bucket)){
    stop("Bucket does not exist")
  }

  splitted = suppressWarnings(split(data, seq(1:split_files)))

  message(paste("Uploading", split_files, "files with prefix", prefix, "to bucket", bucket))

  pb <- progress_bar$new(total = split_files, format='Uploading file :current/:total [:bar]')
  pb$tick(0)

  upload_part = function(part, i){

    s3Name <- paste(prefix, ".", formatC(i, width = 4, format = "d", flag = "0"), sep="")

    r=put_object(.data = part, bucket = bucket, key = s3Name)
    pb$tick()
    return(r)
  }

  res = map2(splitted, 1:split_files, upload_part)

  if(length(which(!unlist(res))) > 0){
    warning("Error uploading data!")
    return(NA)
  }else{
    message("Upload to S3 complete!")
    return(prefix)
  }
}

#' @importFrom "purrr" "map"
deletePrefix = function(prefix, bucket, split_files, key, secret, session, region){

  s3Names=paste(prefix, ".", formatC(1:split_files, width = 4, format = "d", flag = "0"), sep="")

  message(paste("Deleting", split_files, "files with prefix", prefix, "from bucket", bucket))

  pb <- progress_bar$new(total = split_files, format='Deleting file :current/:total [:bar]')
  pb$tick(0)

  deleteObj = function(key){
    delete_object(bucket, key = key)
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

splitDetermine = function(dbcon, numRows, rowSize){
  message("Getting number of slices from Redshift")
  slices = queryDo(dbcon,"select count(*) from stv_slices")
  slices_num = pmax(as.integer(round(slices[1,'count'])), 1)
  split_files = slices_num

  bigSplit = pmin(floor((numRows*rowSize)/(256*1024*1024)), 5000) #200Mb Per file Up to 5000 files
  smallSplit = pmax(ceiling((numRows*rowSize)/(10*1024*1024)), 1) #10MB per file, very small files

  if(bigSplit > slices_num){
    split_files=slices_num*round(bigSplit/slices_num) # Round to nearest multiple of slices, optimizes the load
  }else if(smallSplit < slices_num){
    split_files=smallSplit
  }else{
    split_files=slices_num
  }

  message(sprintf("%s slices detected, will split into %s files", slices, split_files))
  return(split_files)
}

s3ToRedshift = function(dbcon, table_name, bucket, prefix, region, access_key, secret_key, session, iam_role_arn, additional_params){
    stageTable=paste0(sample(letters,16),collapse = "")
    # Create temporary table for staging data
    queryStmt(dbcon, sprintf("create temp table %s (like %s)", stageTable, table_name))
    copyStr = "copy %s from 's3://%s/%s.' region '%s' csv ignoreheader 1 emptyasnull COMPUPDATE FALSE STATUPDATE FALSE %s %s"
    # Use IAM Role if available
    if (nchar(iam_role_arn) > 0) {
      credsStr = sprintf("iam_role '%s'", iam_role_arn)
    } else {
      # creds string now includes a token in case it is needed.
        if (session != '') {
          credsStr = sprintf("credentials 'aws_access_key_id=%s;aws_secret_access_key=%s;token=%s'", access_key, secret_key, session)
        } else {
          credsStr = sprintf("credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'", access_key, secret_key)
        }
    }
    statement = sprintf(copyStr, stageTable, bucket, prefix, region, additional_params, credsStr)
    queryStmt(dbcon, statement)

    return(stageTable)
}

#' @importFrom paws s3
bucket_exists <- function(bucket) {

  svc <- s3(
       config = list(
         credentials = list(
           creds = list(
             access_key_id     = Sys.getenv('AWS_ACCESS_KEY_ID'),
             secret_access_key = Sys.getenv('AWS_SECRET_ACCESS_KEY'),
             session_token     = Sys.getenv('AWS_SESSION_TOKEN')
            )
          ),
         region = Sys.getenv('AWS_DEFAULT_REGION')
       )
     )

  response <- tryCatch(
    expr = svc$head_bucket(bucket),
    error = function(e) NULL
  )

  return(!is.null(response))
}

#' @importFrom paws s3
object_exists <- function(bucket, key) {

  svc <- s3(
    config = list(
      credentials = list(
        creds = list(
          access_key_id     = Sys.getenv('AWS_ACCESS_KEY_ID'),
          secret_access_key = Sys.getenv('AWS_SECRET_ACCESS_KEY'),
          session_token     = Sys.getenv('AWS_SESSION_TOKEN')
        )
      ),
      region = Sys.getenv('AWS_DEFAULT_REGION')
    )
  )

  response <- tryCatch(
    expr = svc$head_object(Bucket = bucket, Key = key),
    error = function(e) NULL
  )

  return(!is.null(response))
}

#' @importFrom paws s3
delete_object <- function(bucket, key) {

  if(!object_exists(bucket = bucket, key = key)){
    stop("Object does not exist in the target bucket")
  }

  svc <- s3(
    config = list(
      credentials = list(
        creds = list(
          access_key_id     = Sys.getenv('AWS_ACCESS_KEY_ID'),
          secret_access_key = Sys.getenv('AWS_SECRET_ACCESS_KEY'),
          session_token     = Sys.getenv('AWS_SESSION_TOKEN')
        )
      ),
      region = Sys.getenv('AWS_DEFAULT_REGION')
    )
  )

  response <- tryCatch(
    expr = svc$delete_object(Bucket = bucket, Key = key),
    error = function(e) NULL
  )

  return(!is.null(response))
}

#' @importFrom paws s3
#' @importFrom readr format_csv
put_object <- function(.data, bucket, key) {

  svc <- s3(
    config = list(
      credentials = list(
        creds = list(
          access_key_id     = Sys.getenv('AWS_ACCESS_KEY_ID'),
          secret_access_key = Sys.getenv('AWS_SECRET_ACCESS_KEY'),
          session_token     = Sys.getenv('AWS_SESSION_TOKEN')
        )
      ),
      region = Sys.getenv('AWS_DEFAULT_REGION')
    )
  )

  response <- tryCatch(
    expr = .data %>%
             format_csv(na = "") %>%
             charToRaw() %>%
             svc$put_object(Body = ., Bucket = bucket, Key = key),
    error = function(e) NULL
  )

  return(!is.null(response))
}
