#' Insert Rows
#' 
#' Insert rows with a selection of columns into already existing tables. Unlike \code{dbWriteTable}, this function will insert rows with an incomplete column selection. Seems to handle most datatypes, but no guarantees.
#' 
#' @param conn the database connection.
#' @param table the table name, possibly including the schema
#' @param value the local value to write
#' @colnames the column names, if different than the names in \code{value}
#' @importFrom RPostgreSQL dbGetQuery
#' @export
dbInsert <- function(conn, table, value, colnames = names(value)){
  stmt <- paste0("INSERT INTO ", table, "(", paste0(colnames, collapse = ", "), ") VALUES ")
  fn <- function(x) {
    if(("numeric" %in% class(x)) | ("integer" %in% class(x))){
      x <- as.character(x)
    } else if ("logical" %in% class(x)){
      x <- ifelse(x, "true", "false")
    } else {
      x <- paste0("'", as.character(x), "'")
    }
    
    # handle NA
    x <- sapply(x, function(x) ifelse(is.na(x), "NULL", x))
    return(x)
  }
  
  for(i in 1:ncol(value)){
    value[,i] <- fn(value[,i])
  }
  rstmt <- paste0(apply(X=value, MARGIN=1, function(x) paste("(", paste0(as.character(x), collapse = ", "), ")")), collapse = ", ")
  stmt = paste0(stmt, rstmt, ";")
  dbGetQuery(conn = conn, statement=stmt)
}
