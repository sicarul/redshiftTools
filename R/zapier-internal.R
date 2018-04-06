#' Create a mini event_cube
#'
#' @param db_fun A R6DB Function
#' @param name The event name
#' @param start Start Period
#' @param end End Period
#' @param ... Options passed to redshiftTools::ctas to create the table
#' @param additional_processing Defined as functional chain, e.g. . %>% identity
#'
#' @return
#' @export
#'
#' @examples
mini_cube <- function(db_fun, name, start, end, resulting_table_name, ..., additional_processing = . %>% {.}) {
  mini_cube_event <- paste0("tt_", gsub("-", "", generate_uuids(1)))
  if (is.null(resulting_table_name)) {
    resulting_table_name <- paste0("tt_", gsub("-", "", generate_uuids(1)))
  }

  db_fun("select * from event where name = '{{name}}' and timestamp >= '{{start}}' and timestamp < '{{end}}'",
         list(
           name = name,
           start = start,
           end = end
         )) %>%
    ctas(mini_cube_event, diststyle = "key", distkey = "id", compound_sort = "id")
  db_fun("select * from {{mini_cube_event}}", list(mini_cube_event=mini_cube_event)) %>%
    left_join(
      db_fun("select * from event_property") %>%
        rename(id = event_id)
    ) %>%
    additional_processing %>%
    ctas(resulting_table_name, ...)
  return(db_fun("select * from {{resulting_table_name}}", list(resulting_table_name = resulting_table_name)))
}
