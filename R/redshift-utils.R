#' Find recent redshift errors
#'
#' Newest errors at the top
#'
#' @param con Redshift connection
#' @param n Number of errors to show
#'
#' @return data.frame
#' @export
#' @importFrom whisker whisker.render
#' @importFrom DBI dbGetQuery
#' @importFrom stringr str_trim
#' @importFrom dplyr arrange desc select collect
#' @importFrom purrr map
recent_errors <- function(con, n = 10) {
  dbGetQuery(con, whisker.render("select colname, type, raw_field_value, err_reason, starttime from stl_load_errors limit {{limit}}",
        list(
          limit = n
        )
  )) %>% arrange(desc(starttime)) %>%
    select(-starttime) %>%
    collect(n = Inf) %>%
    purrr::map(stringr::str_trim) %>%
    as.data.frame()
}


#' Show definition for a view
#'
#' @param con
#' @param view_name
#'
#' @return character
#' @export
#'
#' @examples
#' @importFrom whisker whisker.render
#' @importFrom DBI dbGetQuery
view_definition <- function(con, view_name) {
  dbGetQuery(con, whisker.render("select view_definition from information_schema.views where table_name = '{{view_name}}'", list(view_name = view_name)))
}

