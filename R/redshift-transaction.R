#' Issue commands within a single transaction block.
#'
#' @param .data a data.frame
#' @param .dbcon a DBI connection
#' @param .function_sequence a list of functions to apply, BEWARE the order matters. They'll be run head to tail.
#'
#' @return boolean
#' @export
#'
#' @examples
#' \dontrun{
#' transaction(
#' .data = mtcars,
#' .dbcon = rs$con,
#' .function_sequence = list(
#'  function(...) { rs_create_table(table_name = "mtcars", ...) },
#'  function(...) { rs_upsert_table(tableName = "mtcars", ...) },
#'  function(...) { rs_replace_table(tableName = "mtcars", ...) }
#'  )
#' )
#' }
#'
transaction <- function(.data, .dbcon, .function_sequence) {
  result <- tryCatch(
    {
      message("Beginning transaction")
      if ("pqConnection" %in% class(.dbcon)) {
        DBI::dbBegin(.dbcon)
        warning("pqConnection is going to give you a bad time")
      } else {
        DBI::dbGetQuery(.dbcon, "BEGIN;")
      }

      lapply(.function_sequence, function(.f) {
        .f(.data, .dbcon, use_transaction = FALSE)
      })

      message("Committing changes")
      DBI::dbExecute(.dbcon, "COMMIT;")
      TRUE
    },
    error = function(e) {
      message(e$message)
      DBI::dbExecute(.dbcon, "ROLLBACK;")
      message("Rollback complete")
      FALSE
    }
  )
  if (is.null(result) || !isTRUE(result)) {
    stop("A redshift error occured")
  }
  return(result)
}
