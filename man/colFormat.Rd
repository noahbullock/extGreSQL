% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/colFormat.R
\name{colFormat}
\alias{colFormat}
\title{SQL column conversion}
\usage{
colFormat(x)
}
\arguments{
\item{x}{the column or vector to be formatted}
}
\value{
a \code{character} vector that should load properly.
}
\description{
Convert \code{data.frame} columns to a string format appropriate to the data type for multi-row INSERT. WARNING: while this appears to handle most data types pretty well, it's a work-in-progress. No guarantees. Mostly for internal use.
}

