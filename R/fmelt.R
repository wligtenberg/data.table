# Add melt generic, don't import reshape2, it requires R >= 3.0.0.
melt <- function(data, ..., na.rm = FALSE, value.name = "value") {
  UseMethod("melt", data)
}

melt.data.table <- function(data, id.vars, measure.vars, variable.name = "variable", 
           value.name = "value", ..., na.rm = FALSE, variable.factor = TRUE, value.factor = FALSE, 
           verbose = getOption("datatable.verbose")) {
    if (!is.data.table(data)) stop("'data' must be a data.table")
    if (missing(id.vars)) id.vars=NULL
    if (missing(measure.vars)) measure.vars = NULL
    if (is.list(measure.vars)) {
        if (length(variable.name) == 1L) variable.name = paste(variable.name, seq_along(measure.vars), sep="")
        if (length(value.name) == 1L)  value.name = paste(value.name, seq_along(measure.vars), sep="")
    }
    ans <- .Call("Cfmelt", data, id.vars, measure.vars, 
            as.logical(variable.factor), as.logical(value.factor), 
            variable.name, value.name, as.logical(na.rm), 
            as.logical(verbose));
    setDT(ans)
    if (any(duplicated(names(ans)))) {
        message("Duplicate column names found in molten data.table. Setting unique names using 'make.names'")   
        setnames(ans, make.unique(names(ans)))
    }
    setattr(ans, 'sorted', NULL)
    ans
}

# Redirect to reshape2's melt
melt.data.frame <- function(...) {
    reshape2:::melt.data.frame(...)
}

melt.array <- function(...) {
    reshape2:::melt.array(...)
}

melt.list <- function(...) {
    reshape2:::melt.list(...)
}

melt.table <- function(...) {
    reshape2:::melt.table(...)
}

melt.matrix <- function(...) {
    reshape2:::melt.matrix(...)
}

melt.default <- function(...) {
    reshape2:::melt.default(...)
}
