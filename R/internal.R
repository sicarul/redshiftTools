# Internal utility functions used by the redshift tools

#' @importFrom "aws.s3" "put_object" "bucket_exists"
#' @importFrom "utils" "write.csv"
uploadToS3 <- function(data, bucket, split_files) {

  prefix = paste0(sample(letters, 16), collapse = "")

  if(!bucket_exists(bucket)) {
    stop("Bucket does not exist")
  }

  if(nrow(data) == 0) {
    stop("Input data is empty")
  }

  if(nrow(data) < split_files) {
    split_files <- nrow(data)
  }

  splitted = suppressWarnings(split(data, seq(1:split_files)))

  for(i in 1:split_files) {

    part <- data.frame(splitted[i])

    tmpFile <- tempfile()
    s3Name <- paste0(paste(prefix, ".", formatC(i, width = 4, format = "d", flag = "0"), sep = ""), ".psv.gz")
    # http://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-format.html
    # The default redshift delimiter is a pipe, "|"
    write.table(part, gzfile(tmpFile), sep = "|", na = "", row.names = F, col.names = T,
                # ending a line with the delimiter is required, otherwise stl_load_errors
                # will return: Delimiter not found
                eol = "|\n")

    print(paste("Uploading", s3Name))
    put_object(file = tmpFile, object = s3Name, bucket = bucket)
  }

  return(prefix)
}

#' @importFrom "aws.s3" "delete_object"
deletePrefix <- function(prefix, bucket, split_files){
  for(i in 1:split_files) {
    s3Name = paste(prefix, ".", formatC(i, width = 4, format = "d", flag = "0"), sep="")
    print(paste("Deleting", s3Name))
    delete_object(s3Name, bucket)
  }
}

#' @importFrom DBI dbGetQuery
queryDo <- function(dbcon, query){
  dbGetQuery(dbcon, query)
}

