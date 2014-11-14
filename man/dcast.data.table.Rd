\name{dcast.data.table}
\alias{dcast.data.table}
\alias{dcast}
\title{Fast dcast for data.table}
\description{
  \code{dcast.data.table} is a much faster version of \code{reshape2::dcast}, but for \code{data.table}s. More importantly, it's also capable of handling very large data quite efficiently in terms of memory usage in comparison to \code{reshape2}. 

  The only functionality yet to be implemented from \code{reshape2} is \code{margins}.
  
  In \code{1.9.8}, a lot of other new features were implemented - casting on multiple columns and/or multiple aggregation functions siumultaneously, expressions in formula and preserving column attributes in the output. Check out the \code{Details} and \code{Examples} sections for more info.

}
% \method{dcast}{data.table}
\usage{
## fast dcast a data.table (not an S3 method yet)
dcast.data.table(data, formula, fun.aggregate = NULL, 
    ..., margins = NULL, subset = NULL, fill = NULL, 
    drop = TRUE, value.var = guess(data),
    verbose = getOption("datatable.verbose"))
}
\arguments{
  \item{data}{ A molten \code{data.table} object, see \code{\link{melt.data.table}}.}
  \item{formula}{A formula of the form LHS ~ RHS to cast, see details. \bold{NEW:} Wrap expressions with \code{.()}, for e.g., a + b ~ .(paste("col", c, sep="_"))}
  \item{fun.aggregate}{Should the data be aggregated before casting? If the formula doesn't identify single observation for each cell, then aggregation defaults to \code{length} with a message. \bold{NEW:} You can provide multiple aggregation functions wrapped with \code{funs()}. See \code{Details} and \code{Examples} for more.}
  \item{...}{Any other arguments that maybe passed to the aggregating function.}
  \item{margins}{Not implemented yet. Should take variable names to compute margins on. A value of \code{TRUE} would compute all margins.}
  \item{subset}{Specified if casting should be done on subset of the data. Ex: subset = .(col1 <= 5) or subset = .(variable != "January").}
  \item{fill}{Value to fill missing cells with. If \code{fun.aggregate} is present, takes the value by applying the function on 0-length vector.}
  \item{drop}{\code{FALSE} will cast by including all missing combinations.}
  \item{value.var}{Name of the column whose values will be filled to cast. Function \code{guess()} tries to, well, guess this column automatically, if none is provided. \bold{NEW:} Now you can provide more than one column to cast simultaneously. See \code{Details} and \code{Examples} for more.}
  \item{verbose}{Not used yet. Maybe dropped in the future or used to provide information messages onto the console.}
}
\details{
The cast formula takes the form \code{LHS ~ RHS}	, ex: \code{var_x + .. ~ var_y + ..}. The order of entries in the formula is essential. There are two special variables: \code{.} and \code{...}. Their functionality is identical to that of \code{reshape2:::dcast}. 

If the formula's LHS contains the same column more than once, ex: \code{dcast.data.table(DT, x+x~ y)}, then the answer will have duplicate names. In those cases, the duplicate names are renamed using \code{make.unique} so that key can be set without issues.

When variable combinations in \code{formula} doesn't identify a unique value in a cell, \code{fun.aggregate} will have to be specified, which defaults to \code{length} if unspecified. The aggregating function should take a vector as input and return a single value (or a list of length one) as output. In cases where the column specified in \code{value.var} is of type list, the function should be able to handle a list input and provide a single value or list of length one as output.

From \code{1.9.8}, formulas also handle \emph{expressions} when wrapped within \code{.()}. For example, \code{ a + b ~ .(paste("col", c, sep="_")) } will obtain the column by pasting together \code{"col"} to column \code{c} before to cast.

In addition, \code{1.9.8} also enables casting more than one \code{value.var} column and also allows multiple aggregation functions in the same call. To cast more than one \code{value.var} column, provide all column names, for e.g., \code{value.var = c("d", "e")}. And when more than one function needs to be provided, they should be wrapped within \code{funs()}. Aggregation function can be limited to specific \code{value.var} columns by specifiying \code{vars} argument, while wrapping the function within \code{.()}. 

Here's an example: \code{dcast.data.table(DT, a + b ~ c, fun.aggregate = funs(mean, .(function(x) paste(x, collapse=""), vars="e"), .(sum, vars="d"))), value.var=c("d", "e"))} - this calculates \code{mean} on both columns \code{d} and \code{e}, \code{paste} on column \code{e} alone, and \code{sum} on column \code{d} alone. 

Names for columns that are being cast are generated in the same order (separated by a \code{_}) from the (unique) values in each column mentioned in the formula RHS. In \code{1.9.8}, when more than one \code{value.var} column or function to aggregate by are provided, the column names are automatically appended with the function name and corresponding \code{value.var} name.

Since \code{1.9.4}, \code{dcast.data.table} preserves attributes whereever possible, except for \code{factor} columns. Since \code{1.9.8}, it preserves attributes on \code{factor} and \code{ordered factor} columns as well.

}
\value{
    A keyed \code{data.table} that has been cast. The key columns are equal to the variables in the \code{formula} LHS in the same order.
}

\examples{
require(data.table)
names(ChickWeight) <- tolower(names(ChickWeight))
DT <- melt(as.data.table(ChickWeight), id=2:4) # calls melt.data.table

# no S3 method yet, have to use "dcast.data.table"
dcast.data.table(DT, time ~ variable, fun=mean)
dcast.data.table(DT, diet ~ variable, fun=mean)
dcast.data.table(DT, diet+chick ~ time, drop=FALSE)
dcast.data.table(DT, diet+chick ~ time, drop=FALSE, fill=0)

# using subset
dcast.data.table(DT, chick ~ time, fun=mean, subset=.(time < 10 & chick < 20))

\dontrun{
# on big data
set.seed(45)
DT <- data.table(aa=sample(1e4, 1e6, TRUE), 
      bb=sample(1e3, 1e6, TRUE), 
      cc = sample(letters, 1e6, TRUE), dd=runif(1e6))
system.time(dcast.data.table(DT, aa ~ cc, fun=sum)) # 0.28 seconds
system.time(dcast.data.table(DT, bb ~ cc, fun=mean)) # 0.08 seconds
# reshape2::dcast takes 99 seconds
system.time(dcast.data.table(DT, aa + bb ~ cc, fun=sum)) # 2.7 seconds
}

# Examples for new features in 1.9.8
set.seed(45L)
DT = data.table(A = c(1,1,1,1,1,2,2,2,2,2,2,2), 
     B = sample(letters[1:2], 12, TRUE), 
     C = sample(paste("V", 1:3, sep=""),12,TRUE), 
     D = factor(month.abb, levels=month.abb), 
     val1 = sample(c(NA,100:104), 12, TRUE), 
     val2 = sample(letters, 12, TRUE))

# multiple value.vars
dcast.data.table(DT, A + B ~ D, value.var=c("val1", "val2"))

# multiple fun.aggregate, sum with na.rm=TRUE *and* mean for 'val1'
dcast.data.table(DT, A + B ~ C, fun.agg=funs(mean, 
      .(function(x) sum(x, na.rm=TRUE))), value.var="val1")

# mean on val1 and paste on val2
dcast.data.table(DT, A + B ~ C, fun.agg= funs(.(mean, vars="val1"), 
     .(function(x) paste(x, collapse=""), vars="val2")), 
     value.var= c("val1", "val2"))
}
\seealso{
  \code{\link{melt.data.table}}, \url{http://had.co.nz/reshape/}
}
\keyword{data}

