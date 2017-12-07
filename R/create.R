#' Create a table from scratch, guessing the table schema
#'
#'
#' @param data a data frame
#' @param dbcon an RPostgres connection to the redshift server
#' @param table_name the name of the table to create
#' @param split_files optional parameter to specify amount of files to split into. If not specified will look at amount of slices in Redshift to determine an optimal amount.
#' @param bucket the name of the temporary bucket to load the data. Will look for AWS_BUCKET_NAME on environment if not specified.
#' @param region the region of the bucket. Will look for AWS_DEFAULT_REGION on environment if not specified.
#' @param access_key the access key with permissions for the bucket. Will look for AWS_ACCESS_KEY_ID on environment if not specified.
#' @param secret_key the secret key with permissions fot the bucket. Will look for AWS_SECRET_ACCESS_KEY on environment if not specified.
#' @param iam_role_arn an iam role arn with permissions fot the bucket. Will look for AWS_IAM_ROLE_ARN on environment if not specified. This is ignoring access_key and secret_key if set.
#' @param wlm_slots amount of WLM slots to use for this bulk load http://docs.aws.amazon.com/redshift/latest/dg/tutorial-configuring-workload-management.html
#' @param sortkeys Column or columns to sort the table by
#' @param sortkey_style Sortkey style, can be compound or interleaved http://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data-compare-sort-styles.html
#' @param distkey Distkey column, can only be one, if chosen the table is distributed among clusters according to a hash of this column's value.
#' @param distkey_style Distkey style, can be even or all, for the key distribution use the distkey parameter. http://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html
#' @param compression Add encoding for columns whose compression algorithm is easy to guess, for the rest you should upload it to Redshift and run ANALYZE COMPRESSION
#'
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
#' rs_create_table(data=a, dbcon=con, table_name='testTable',
#' bucket="my-bucket", split_files=4)
#'
#' }
#' @export
rs_create_table = function(
    data,
    dbcon,
    table_name=deparse(substitute(data)),
    split_files,
    bucket=Sys.getenv('AWS_BUCKET_NAME'),
    region=Sys.getenv('AWS_DEFAULT_REGION'),
    access_key=Sys.getenv('AWS_ACCESS_KEY_ID'),
    secret_key=Sys.getenv('AWS_SECRET_ACCESS_KEY'),
    iam_role_arn=Sys.getenv('AWS_IAM_ROLE_ARN'),
    wlm_slots=1,
    sortkeys,
    sortkey_style='compound',
    distkey,
    distkey_style='even',
    compression=T
    )
  {

  tableSchema = rs_create_statement(data, table_name = table_name, sortkeys=sortkeys,
  sortkey_style = sortkey_style, distkey=distkey, distkey_style = distkey_style,
  compression = compression)

  queryStmt(dbcon, tableSchema)

  return(rs_replace_table(data, dbcon, table_name, split_files, bucket, region, access_key, secret_key, iam_role_arn, wlm_slots))

}
