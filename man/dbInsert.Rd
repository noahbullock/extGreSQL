\name{dbInsert}
\alias{dbInsert}
\title{Insert Rows}
\usage{
  dbInsert(conn, table, value, colnames = names(value))
}
\arguments{
  \item{conn}{the database connection.}

  \item{table}{the table name, possibly including the
  schema}

  \item{value}{the local value to write}
}
\description{
  Insert rows with a selection of columns into already
  existing tables. Unlike \code{dbWriteTable}, this
  function will insert rows with an incomplete column
  selection. Seems to handle most datatypes, but no
  guarantees.
}

