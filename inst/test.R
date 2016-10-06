library(zapieR)
library(DBI)
library(redshiftTools)
library(magrittr)
make_db_connections()

dbGetQuery(rs2, "drop table if exists iris")
iris$Species <- as.character(iris$Species)
names(iris) <- gsub(".", "_", names(iris), fixed = TRUE)
original <- iris[1:10, -1]
new <- iris[11:20,-2]
original %T>%
  rs_create_table(rs$con, "iris") %>%
  rs_upsert_table(rs$con, "iris",bucket = "zapier-data-science-storage")

new %>%
  rs_upsert_table(rs$con, "iris",bucket = "zapier-data-science-storage", strict = FALSE)

