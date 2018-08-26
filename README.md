<!-- README.md is generated from README.Rmd. Please edit that file -->
redshiftTools
=============

This is an R Package meant to easen common operations with Amazon Redshift. The first motivation for this package was making it easier for bulk uploads, where the procedure for uploading data consists in generating various CSV files, uploading them to an S3 bucket and then calling a copy command on the server, this package helps with all those tasks in encapsulated functions.

Installation
------------

To install this package, you'll need to execute these commands:

``` r
install.packages("aws.ec2metadata", repos = c(cloudyr = "http://cloudyr.github.io/drat", getOption("repos")))
packages = c("devtools", "httr", "package_n","aws.s3", "Rcpp","DBI","data.table","future","future.apply","R.utils","dplyr")
for(pack in packages){
  if(!pack %in% rownames(installed.packages())){
    print(paste("installing",pack))
    install.packages(pack)
  }
}
devtools::install_github("sicarul/redshiftTools")
```

Drivers
-------

This library supports two official ways of connecting to Amazon Redshift (Others may work, but untested):

### RPostgres

This Postgres library is great, and it works even with Amazon Redshift servers with SSL enabled. It previously didn't support transactions, but is now the recommended way to work with redshiftTools.

To use it, please configure like this:

``` r
    devtools::install_github("r-dbi/RPostgres")
    library(RPostgres)
    
    con <- dbConnect(RPostgres::Postgres(), dbname="dbname",
    host='my-redshift-url.amazon.com', port='5439',
    user='myuser', password='mypassword',sslmode='require')
    test=dbGetQuery('select 1')
```

### RJDBC

If you download the official redshift driver .jar, you can use it with this R library, it's not great in the sense that you can't use it with dplyr for example, since it doesn't implement all the standard DBI interfaces, but it works fine for uploading data.

To use it, please configure like this:

``` r
    install.packages('RJDBC')
    library(RJDBC)
    
    # Save the driver into a directory
    dir.create('~/.redshiftTools')
    download.file('http://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC41-1.1.9.1009.jar','~/.redshiftTools/redshift-driver.jar')
    
    # Use Redshift driver
    driver <- JDBC("com.amazon.redshift.jdbc41.Driver", "~/.redshiftTools/redshift-driver.jar", identifier.quote="`")

    # Create connection    
    dbname="dbname"
    host='my-redshift-url.amazon.com'
    port='5439'
    user='myuser'
    password='mypassword'
    ssl='true'
    url <- sprintf("jdbc:redshift://%s:%s/%s?tcpKeepAlive=true&ssl=%s&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory", host, port, dbname, ssl)
    conn <- dbConnect(driver, url, user, password)
```

Usage
-----

### Creating tables

For creating tables, there is a support function, `rs_create_statement`, which receives a data.frame and returns the query for creating the same table in Amazon Redshift.

``` r
n=1000
testdf = data.frame(
a=rep('a', n),
b=c(1:n),
c=rep(as.Date('2017-01-01'), n),
d=rep(as.POSIXct('2017-01-01 20:01:32'), n),
e=rep(as.POSIXlt('2017-01-01 20:01:32'), n),
f=rep(paste0(rep('a', 4000), collapse=''), n) )

cat(rs_create_statement(testdf, table_name='dm_great_table'))
```

This returns:

``` sql
CREATE TABLE dm_great_table (
a VARCHAR(8),
b int,
c date,
d timestamp,
e timestamp,
f VARCHAR(4096)
);
```

The cat is only done to view properly in console, it's not done directly in the function in case you need to pass the string to another function (Like a query runner)

### Uploading data

For uploading data, you'll have available now 2 functions: `rs_replace_table` and `rs_upsert_table`, both of these functions are called with almost the same parameters, except on upsert you can specify with which keys to search for matching rows.

For example, suppose we have a table to load with 2 integer columns, we could use the following code:

``` r
    library("aws.s3")
    library(RPostgres)
    library(redshiftTools)

    a=data.frame(a=seq(1,10000), b=seq(10000,1))
    n=head(a,n=10)
    n$b=n$a
    nx=rbind(n, data.frame(a=seq(5:10), b=seq(10:5)))

    con <- dbConnect(RPostgres::Postgres(), dbname="dbname",
    host='my-redshift-url.amazon.com', port='5439',
    user='myuser', password='mypassword',sslmode='require')

    b=rs_replace_table(a, dbcon=con, table_name='mytable', bucket="mybucket", split_files=4)
    c=rs_upsert_table(nx, dbcon=con, table_name = 'mytable', split_files=4, bucket="mybucket", keys=c('a'))
```

### Creating tables with data

A conjunction of `rs_create_statement` and `rs_replace_table` can be found in `rs_create_table`. You can create a table from scratch from R and upload the contents of the data frame, without needing to write SQL code at all.

``` r
    library("aws.s3")
    library(RPostgres)
    library(redshiftTools)

    a=data.frame(a=seq(1,10000), b=seq(10000,1))
    
    con <- dbConnect(RPostgres::Postgres(), dbname="dbname",
    host='my-redshift-url.amazon.com', port='5439',
    user='myuser', password='mypassword',sslmode='require')

    b=rs_create_table(a, dbcon=con, table_name='mytable', bucket="mybucket", split_files=4)
    
```
