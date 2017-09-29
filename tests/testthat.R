library(testthat)
library(redshiftTools)
library(zapieR)
library(lubridate)
library(dbplyr)

test_check("redshiftTools")
Sys.setenv(REDSHIFT_ROLE = 'arn:aws:iam::996097627176:role/production-redshift')
