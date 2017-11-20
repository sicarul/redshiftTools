#' @importFrom "utils" "head"
calculateCharSize <- function(col){
  col=as.character(col)
  maxChar = max(nchar(col), na.rm=T)
  sizes = 2^c(3:16) # From 8 to 65536, max varchar size in redshift
  fsizes = sizes[ifelse(sizes>maxChar, T, F)]
  if(length(fsizes)==0){
    warning("Character column over maximum size of 65536, set to that value but will fail if not trimmed before uploading!")
    warning(paste0('Example offending value: ', head(col[nchar(col) > 65536], 1)))
    return(max(sizes))
  }else{
    return(min(fsizes))
  }
}

colToRedshiftType <- function(col) {
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
  #
  return(paste0('VARCHAR(', calculateCharSize(col), ')'))
}


getRedshiftTypesForDataFrame <- function(df) {
  return(
    sapply(
      df,
      FUN = colToRedshiftType
    )
  )
}

#' Generate create table statement for Amazon Redshift
#'
#' This lets you easily generate a table schema from a data.frame, which allows for easily uploading to redshift afterwards.
#'
#' @param df the data.frame you want to upload to Amazon Redshift
#' @param tableName the name of the table to create, if not specified it'll use the data.frame name
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
#'cat(rs_create_statement(testdf, tableName='dm_great_table'))
#'
#' @export
rs_create_statement <- function(df, tableName = deparse(substitute(df))){
  definitions = getRedshiftTypesForDataFrame(df)
  fields = paste(names(definitions), definitions, collapse=',\n')

  if(ncol(df) > 1600){
    warning("Redshift doesn't support tables of more than 1600 columns")
  }

  return(paste0('CREATE TABLE ', tableName, ' (\n', fields, '\n);'))
}
