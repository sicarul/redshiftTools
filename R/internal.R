# Internal utility functions used by the redshift tools

#' @importFrom "aws.s3" "put_object" "bucket_exists"
#' @importFrom "utils" "write.csv"
uploadToS3 <- function(data, bucket, split_files) {

  prefix = paste0(sample(letters, 32, replace = TRUE), collapse = "")

  if(!bucket_exists(bucket)) {
    stop("Bucket does not exist")
  }

  if(nrow(data) == 0) {
    stop("Input data is empty")
  }

  if(nrow(data) < split_files) {
    split_files <- nrow(data)
  }

  splitted <- suppressWarnings(split(data, seq(1:split_files)))
  parallel::mclapply(1:split_files, function(i) {

    part <- data.frame(splitted[i])

    tmpFile <- tempfile()
    tmpFile <- paste0(tmpFile, ".psv")
    s3Name <- paste0(paste(prefix, ".", formatC(i, width = 4, format = "d", flag = "0"), sep = ""), ".psv.gz")
    # http://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-format.html
    # The default redshift delimiter is a pipe, "|"
    part <- as.data.frame(lapply(part, function(y) gsub('"', "", y)))
    part <- as.data.frame(lapply(part, function(y) gsub("'", "", y)))
    part <- as.data.frame(lapply(part, function(y) gsub("\\\\", "", y)))
    part <- as.data.frame(lapply(part, function(y) gsub("\\|", "\\\\|", y)))
    part <- as.data.frame(lapply(part, function(y) gsub('\\n', "", y)))
    data.table::fwrite(part, tmpFile, sep = "|", na = "", col.names = T,
                       # ending a line with the delimiter is required, otherwise stl_load_errors
                       # will return: Delimiter not found
                       eol = "|\n")

    system(paste("gzip", tmpFile))

    print(paste("Uploading", s3Name))
    s3 <- switch(bucket,
           `zapier-data-science-storage` = data_science_storage_s3(),
           `data-monolith-etl` = data_monolith_etl_s3(),
          data_science_storage_s3()
    )
    s3$set_file(object = s3Name, file = paste0(tmpFile, ".gz"))
  })

  return(prefix)
}

#' @importFrom "aws.s3" "delete_object"
#' @importFrom parallel mclapply
deletePrefix <- function(prefix, bucket, split_files){
  parallel::mclapply(1:split_files, function(i) {
    s3Name = paste(prefix, ".", formatC(i, width = 4, format = "d", flag = "0"), ".psv.gz", sep="")
    delete_object(s3Name, bucket)
  })
}

#' @importFrom DBI dbGetQuery
queryDo <- function(dbcon, query){
  dbGetQuery(dbcon, query)
}

RESERVED_WORDS <- readLines(textConnection("AES128
AES256
ALL
ALLOWOVERWRITE
ANALYSE
ANALYZE
AND
ANY
ARRAY
AS
ASC
AUTHORIZATION
BACKUP
BETWEEN
BINARY
BLANKSASNULL
BOTH
BYTEDICT
BZIP2
CASE
CAST
CHECK
COLLATE
COLUMN
CONSTRAINT
CREATE
CREDENTIALS
CROSS
CURRENT_DATE
CURRENT_TIME
CURRENT_TIMESTAMP
CURRENT_USER
CURRENT_USER_ID
DEFAULT
DEFERRABLE
DEFLATE
DEFRAG
DELTA
DELTA32K
DESC
DISABLE
DISTINCT
DO
ELSE
EMPTYASNULL
ENABLE
ENCODE
ENCRYPT
ENCRYPTION
END
EXCEPT
EXPLICIT
FALSE
FOR
FOREIGN
FREEZE
FROM
FULL
GLOBALDICT256
GLOBALDICT64K
GRANT
GROUP
GZIP
HAVING
IDENTITY
IGNORE
ILIKE
IN
INITIALLY
INNER
INTERSECT
INTO
IS
ISNULL
JOIN
LEADING
LEFT
LIKE
LIMIT
LOCALTIME
LOCALTIMESTAMP
LUN
LUNS
LZO
LZOP
MINUS
MOSTLY13
MOSTLY32
MOSTLY8
NATURAL
NEW
NOT
NOTNULL
NULL
NULLS
OFF
OFFLINE
OFFSET
OID
OLD
ON
ONLY
OPEN
OR
ORDER
OUTER
OVERLAPS
PARALLEL
PARTITION
PERCENT
PERMISSIONS
PLACING
PRIMARY
RAW
READRATIO
RECOVER
REFERENCES
RESPECT
REJECTLOG
RESORT
RESTORE
RIGHT
SELECT
SESSION_USER
SIMILAR
SOME
SYSDATE
SYSTEM
TABLE
TAG
TDES
TEXT255
TEXT32K
THEN
TIMESTAMP
TO
TOP
TRAILING
TRUE
TRUNCATECOLUMNS
UNION
UNIQUE
USER
USING
VERBOSE
WALLET
WHEN
WHERE
WITH
WITHOUT"))

