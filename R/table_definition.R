#' @importFrom "utils" "head"
calculateCharSize <- function(col){
  col=as.character(col)
  maxChar = max(nchar(col), na.rm=T)
  if(is.infinite(maxChar)){
    maxChar=1000
    warning('Empty column found, setting to 1024 length')
  }

  sizes = c(2^c(3:15),65535) # From 8 to 65535, max varchar size in redshift
  fsizes = sizes[ifelse(sizes>maxChar, T, F)]
  if(length(fsizes)==0){
    warning("Character column over maximum size of 65535, set to that value but will fail if not trimmed before uploading!")
    warning(paste0('Example offending value: ', head(col[nchar(col) > 65535], 1)))
    return(max(sizes, na.rm=T))
  }else{
    return(min(fsizes, na.rm=T))
  }
}

colToRedshiftType <- function(col, compression) {
  class = class(col)[[1]]
  switch(class,
         logical = {
           return('boolean')
         },
         numeric = {
           return('float8')
         },
         integer = {
           if(all(is.na(col))){ #Unknown column, all null
             return('int')
           }
           if(max(col, na.rm = T) < 2000000000){ # Max int is 2147483647 in Redshift
             return('int')
           } else if (max(col, na.rm=T) < 9200000000000000000){ #Max bigint is 9223372036854775807 in redshift, if bigger treat as numeric
             return('bigint')
           } else{
             return('numeric(38,0)')
           }

         },
         Date = {
           return('date')
         },
         POSIXct = {
           return('timestamp')
         },
         POSIXlt = {
           return('timestamp')
         }

  )
  charSize = calculateCharSize(col)
  if(compression==T){
    return(paste0('VARCHAR(', charSize, ') encode zstd'))
  }else{
    return(paste0('VARCHAR(', charSize, ')'))
  }

}


getRedshiftTypesForDataFrame <- function(df, compression) {
  return(
    sapply(
      df,
      FUN = colToRedshiftType,
      compression
    )
  )
}

#' Generate create table statement for Amazon Redshift
#'
#' This lets you easily generate a table schema from a data.frame, which allows for easily uploading to redshift afterwards.
#'
#' @param df the data.frame you want to upload to Amazon Redshift
#' @param table_name the name of the table to create, if not specified it'll use the data.frame name
#' @param sortkeys Column or columns to sort the table by
#' @param sortkey_style Sortkey style, can be compound or interleaved http://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data-compare-sort-styles.html
#' @param distkey Distkey column, can only be one, if chosen the table is distributed among clusters according to a hash of this column's value.
#' @param distkey_style Distkey style, can be even or all, for the key distribution use the distkey parameter. http://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html
#' @param compression Add encoding for columns whose compression algorithm is easy to guess, for the rest you should upload it to Redshift and run ANALYZE COMPRESSION
#' @examples
#'
#'n=1000
#'testdf = data.frame(
#'a=rep('a', n),
#'b=c(1:n),
#'c=rep(as.Date('2017-01-01'), n),
#'d=rep(as.POSIXct('2017-01-01 20:01:32'), n),
#'e=rep(as.POSIXlt('2017-01-01 20:01:32'), n),
#'f=rep(paste0(rep('a', 4000), collapse=''), n) )
#'
#'cat(rs_create_statement(testdf, table_name='dm_great_table'))
#'
#' @export
rs_create_statement <- function(
  df,
  table_name = deparse(substitute(df)),
  sortkeys,
  sortkey_style='compound',
  distkey,
  distkey_style='even',
  compression=T
  ){
  definitions = getRedshiftTypesForDataFrame(df, compression)
  fields = paste(names(definitions), definitions, collapse=',\n')
  sortkey_style=tolower(sortkey_style)
  distkey_style=tolower(distkey_style)

  if(ncol(df) > 1600){
    warning("Redshift doesn't support tables of more than 1600 columns")
  }

  dkey=''
  if(!missing(distkey)){
    dkey=paste0('diststyle key distkey(', distkey, ')\n')
  }else if (distkey_style=='all'){
    dkey=paste0('diststyle all\n')
  }else if (distkey_style!='even'){
    warning('Unknown distkey style', distkey_style)
  }

  skey=''
  if(!missing(sortkeys)){
    if(length(sortkeys) > 1){
      skeyvals = paste(sortkeys, collapse=', ')
      if(!sortkey_style %in% c('interleaved', 'compound')){
        warning('Unknown sortkey style', sortkey_style)
      }
      skey=paste0(sortkey_style, ' sortkey (', skeyvals, ')\n')
    }else{
      skey=paste0('sortkey(', sortkeys,')\n')
    }
  }

  return(paste0('CREATE TABLE ', table_name, ' (\n', fields, '\n)', dkey, skey,';'))
}
