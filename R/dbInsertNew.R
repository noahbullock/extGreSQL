#' Insert New Rows
#' 
#' Insert only those rows without an existing primary key. NOTE: This can cause a race condition under parallel execution. Use at your own risk.
#' 
#' @param conn the database connection.
#' @param table the table name, possibly including the schema
#' @param value the local value to write
#' @param primary.key the primary key or unique value constraint
#' @param colnames the column names, if different than the names in \code{value}
#' @param coltypes column types. If missing, an attempt is made to retrieve them from the database.
#' @param run whether to run (\code{TRUE}) or print (\code{FALSE}) the query.
#' @importFrom RPostgreSQL dbGetQuery
#' @export
dbInsertNew  <- function(conn, table, value, primary.key, colnames = names(value), coltypes, run=TRUE){
  stmt <- paste0("WITH new_values ", "(", paste0(colnames, collapse = ", "), ") AS ( VALUES ")
  if(run){
    if(missing(coltypes)){
      
      tabpieces <- unlist(strsplit(x=table, split="\\."))
      
      if(length(tabpieces) == 1L){
        typesquery <- paste0("select column_name, data_type from information_schema.columns where table_name = '",
                             table, "' AND column_name IN",
                             paste(" (", paste0(paste0("'", colnames, "'"), collapse=", "), ")" ), ";")
      } else if(length(tabpieces) == 2L){
        schm <- tabpieces[1]
        tbl <- tabpieces[2]
        typesquery <- paste0("select column_name, data_type from information_schema.columns where table_name = '",
                             tbl, "' AND table_schema = '", schm, "' AND column_name IN",
                             paste(" (", paste0(paste0("'", colnames, "'"), collapse=", "), ")" ), ";")
      } else {
        stop("Failed to parse schema from 'table' argument. Likely cause: more than one '.' found in name.")
      }
      
      typestab <- dbGetQuery(conn=conn, statement=typesquery)
      coltypes <- typestab[pmatch(colnames, typestab[,1]),2]
    }
    
  }
  
  for(i in 1:ncol(value)){
    value[,i] <- colFormat(value[,i])
    
    if(run){
      value[1,i] <- paste0(value[1,i], "::", coltypes[i])
    } 
  }
  rstmt <- paste0(apply(X=value, MARGIN=1, function(x) paste("(", paste0(as.character(x), collapse = ", "), ")")), collapse = ", ")
  
  stmt <-  paste0(stmt, rstmt, " ) ")
  
  poststmt <- paste0(" INSERT INTO ", table, " (", paste0(colnames, collapse = ", "), 
                     ") SELECT ", paste0("new_values.", colnames, collapse = ", "), 
                     " FROM new_values  WHERE ", primary.key,  " NOT IN ( SELECT ", primary.key, " FROM ",  table, " )")
  
  stmt <- paste0(stmt, poststmt, ";")
  
  if(run){
    dbGetQuery(conn = conn, statement=stmt)
  } else {
    cat(stmt)
  }
  
}
