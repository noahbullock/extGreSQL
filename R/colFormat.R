#' SQL column conversion
#' 
#' Convert \code{data.frame} columns to a string format appropriate to the data type for multi-row INSERT. WARNING: while this appears to handle most data types pretty well, it's a work-in-progress. No guarantees. Mostly for internal use.
#' 
#' @param x the column or vector to be formatted
#' @return a \code{character} vector that should load properly.
#' @export
colFormat <- function(x) {
  if(("numeric" %in% class(x)) | ("integer" %in% class(x))){
    x <- as.character(x)
  } else if ("logical" %in% class(x)){
    x <- ifelse(x, "true", "false")
  } else {
    x <- paste0("'", as.character(x), "'")
  }
  # handle NA. The second argument addresses NA vals that have already been coerced to strings.
  x <- sapply(x, function(y) ifelse(is.na(y) | y == "'NA'", "NULL", y))
  return(x)
}
