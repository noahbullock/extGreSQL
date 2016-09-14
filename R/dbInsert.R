#' Insert Rows
#' 
#' Insert rows with a selection of columns into already existing tables. Unlike \code{dbWriteTable}, this function will insert rows with an incomplete column selection. Seems to handle most datatypes, but no guarantees.
#' 
#' @param conn the database connection.
#' @param table the table name, possibly including the schema
#' @param value the local value to write
#' @param colnames the column names, if different than the names in \code{value}
#' @param statement.only return the statement as a string rather than running the query (default:\code{FALSE})
#' @return If \code{statement.only = FALSE}, the query is run and the return is \code{NULL}. If \code{statement.only = TRUE}, the \code{INSERT} statement is returned as \code{character}.
#' @details If \code{statement.only = TRUE}, the function can be run without a \code{conn} argument. This can be useful for debugging, or generating statements that can be modified with additional code.
#' @importFrom RPostgreSQL dbGetQuery
#' @export
dbInsert <- function(conn, table, value, colnames = names(value), statement.only = FALSE){
  
  if(any(c("tbl_df", "tbl") %in% class(value))){
    value <- as.data.frame(value, stringsAsFactors = FALSE)
  }
  stmt <- paste0("INSERT INTO ", table, "(", paste0(colnames, collapse = ", "), ") VALUES ")
  
  for(i in 1:ncol(value)){
    value[,i] <- colFormat(value[,i])
  }
  rstmt <- paste0(apply(X=value, MARGIN=1, function(x) paste("(", paste0(as.character(x), collapse = ", "), ")")), collapse = ", ")
  stmt = paste0(stmt, rstmt, ";")
  
  if(statement.only){
    stmt
  } else {
    dbGetQuery(conn = conn, statement=stmt)
  }
  
}
