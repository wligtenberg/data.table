guess <- function(x) {
    if ("value" %chin% names(x))
        return("value")
    if ("(all)" %chin% names(x)) 
        return("(all)")
    var <- names(x)[ncol(x)]
    message("Using '", var, "' as value column. Use 'value.var' to override")
    return(var)
}

dcast.data.table <- function(data, formula, fun.aggregate = NULL, ..., margins = NULL, 
    subset = NULL, fill = NULL, drop = TRUE, value.var = guess(data), verbose = getOption("datatable.verbose")) {
    if (!is.data.table(data)) stop("'data' must be a data.table.")
    if (anyDuplicated(names(data))) stop('data.table to cast must have unique column names')
    drop = as.logical(drop[1])
    if (is.na(drop)) stop("'drop' must be logical TRUE/FALSE")
    # formula - UPDATE: formula handles expressions as well
    deparser <- function(call, allvars) {
        lapply(call, function(this) {
            if (is.call(this)) {
                if (this[[1L]] == quote(`+`))
                    unlist(deparser(as.list(this)[-1L], allvars))
                else if (this[[1L]] == quote(`.`))
                    this[[2L]]
                else stop("Cast formula should be of the form LHS ~ RHS, with '+' being the only operator allowed to separate items in LHS and RHS, for e.g., a + b ~ c. Use .() to construct expressions, for e.g., a + b ~ c + .(paste(d, 1:5)).")
            } else if (is.name(this)) {
                if (this == quote(`...`)) {
                    subvars = setdiff(names(data), allvars)
                    lapply(subvars, as.name)
                } else if (this != quote(`.`)) this
            }
        })
    }
    if (is.character(formula)) formula = as.formula(formula)
    if (class(formula) != "formula" || length(formula) != 3L)
        stop("Invalid formula. Cast formula should be of the form LHS ~ RHS, for e.g., a + b ~ c.")
    expr = as.list(formula)[-1L]
    vars_ = unlist(lapply(expr, function(x) { x=all.vars(x); x[!x %chin% c(".", "...")] }))
    expr = deparser(expr, c(vars_, value.var))
    expr_ = unlist(expr)
    setattr(expr, 'names', c("lhs", "rhs"))
    # value.var - UPDATE: more than one value.var possible now.
    if (!is.character(value.var)) 
        stop("value.var must be a character vector (column names) of positive length (> 0).")
    value.var = unique(value.var)
    iswrong = which(!value.var %in% names(data))
    if (length(iswrong))
        stop("value.var values [", paste(value.var[iswrong], collapse=", "), "] are not found in 'data'.")
    val_ = lapply(value.var, as.name)
    # get 'dat'
    dat = lapply(c(expr_, val_), function(x) {
            val = eval(x, data, parent.frame())
            if (is.list(val)) stop("Only 'value.var' column maybe of type list")
            if (is.function(val)) stop("Column [", deparse(x), "] not found or of unknown type.")
            val
        })
    # Have to take care of duplicate names, and provide names for expression columns properly.
    allnames = make.unique(sapply(expr_, function(x) all.names(x, max.names=1L)), sep="_")
    lhsnames = head(allnames, length(expr[[1L]]))
    rhsnames = tail(allnames, -length(expr[[1L]]))
    if (any(value.var %chin% allnames)) {
        value.var = tail(make.unique(c(allnames, value.var)), -length(allnames))
    }
    setattr(dat, 'names', c(allnames, value.var))
    setDT(dat)
    # subset - UPDATE: subset on `dat`, not `data`; `data` may've much more columns not in formula.
    m <- as.list(match.call()[-1L])
    subset <- m[["subset"]][[2L]]
    if (!is.null(subset)) {
        if (is.name(subset)) subset = as.call(list(quote(`(`), subset))
        idx = which(eval(subset, data, parent.frame())) # any advantage thro' secondary keys?
        dat = .Call(CsubsetDT, dat, idx, seq_along(dat))
    }
    if (!nrow(dat) || !ncol(dat)) stop("Can not cast an empty data.table")
    # fun.aggregate - UPDATE: more than one fun.aggregate possible now.
    # if fun.aggregate is NULL, set to 'length' if maxgrp > 1L (to be consistent with reshape2::dcast)
    # E.g., fun = quote(funs(length, .(mean), bla=.(function(x) as.numeric(median(x, na.rm=FALSE)))))
    # E.g., fun = quote(funs(length, .(mean), .(function(x) 1L)))
    # E.g., fun = quote(funs(length, .(mean, vars="d")))
    fun.call = m[["fun.aggregate"]]
    fill.default = NULL
    if (is.null(fun.call)) {
        oo = forderv(dat, by=c(allnames), retGrp=TRUE)
        if (attr(oo, 'maxgrpn') > 1L) {
            message("Aggregate function missing, defaulting to 'length'")
            fun.call = quote(length)
        }
    }
    if (!is.null(fun.call)) {
        cast_funs <- function(fun, allvars, ...) {
            funname = names(fun); fun = fun[[1L]]
            dots = list(...)
            fsymbol <- function(fun, vars) {
                allfuns = lapply(vars, function(x) {
                    expr = list(fun, as.name(x))
                    if (length(dots)) expr = c(expr, dots)
                    as.call(expr)
                })
                setattr(allfuns, 'names', paste(if (is.null(funname) || funname == "") 
                    all.names(fun)[1L] else funname, vars, sep="_"))
            }
            if (is.name(fun)) fun = fsymbol(fun, allvars)
            else if (is.call(fun)) {
                if (fun[[1L]] == ".") {
                    thisvars = eval(fun[["vars"]])
                    fun = fun[[2L]]
                } else thisvars = NULL
                if (is.null(thisvars)) thisvars = allvars
                else if (length(absent <- which(!thisvars %chin% allvars)))
                    stop("Columns specified in 'vars' argument should be present in 'value.var'. Values [", paste(thisvars[absent], collapse=", "), "] are not present.")
                fun = fsymbol(fun, thisvars)
            } else stop("Invalid format for function in fun.aggregate. Please see DETAILS and EXAMPLE sections of ?dcast.data.table.")
            fun
        }
        if (is.call(fun.call) && fun.call[[1L]] == "eval") fun.call = eval(fun.call[[2L]], parent.frame(), parent.frame())
        fun.list = if (is.call(fun.call) && fun.call[[1L]] == "funs") as.list(fun.call)[-1L] else list(fun.call)
        fun.list = unlist(lapply(seq_along(fun.list), function(i) cast_funs(fun.list[i], value.var, ...)), use.names=TRUE)
        fun.call = as.call(c(quote(list), fun.list))

        # fill argument : UPDATE: much easier way to get this done.
        errmsg = "Aggregating function(s) should take vector inputs and return a single value (length=1). However, function(s) returns length!=1. This value will have to be used to fill any missing combinations, and therefore must be length=1. Either override by setting the 'fill' argument explicitly or modify your function to handle this case appropriately."
        if (is.null(fill)) {
            tryCatch(fill.default <- dat[0][, eval(fun.call)], warning = function(x) stop(errmsg, call.=FALSE))
            if (nrow(fill.default) != 1L) stop(errmsg, call.=FALSE)
        }
        if (!any(value.var %chin% allnames)) {
            dat = dat[, eval(fun.call), by=c(allnames)]
        } else {
            dat = dat[, { .SD; eval(fun.call) }, by=c(allnames), .SDcols = value.var]
        }
    }
    order_ <- function(x) {
        o = forderv(x, retGrp=TRUE, sort=TRUE)
        idx = attr(o, 'starts')
        if (!length(o)) o = seq_along(x)
        o[idx] # subsetVector retains attributes, using R's subset for now
    }

    cj_uniq <- function(DT) {
        do.call("CJ", lapply(DT, function(x) 
            if (is.factor(x)) {
                xint = seq_along(levels(x))
                setattr(xint, 'levels', levels(x))
                setattr(xint, 'class', class(x))
            } else .Call(CsubsetVector, x, order_(x))
    ))}
    valnames = setdiff(names(dat), allnames)
    # 'dat' != 'data'? then setkey to speed things up (slightly), else ad-hoc (for now). Still very fast!
    if (!is.null(fun.call) || !is.null(subset)) 
        setkeyv(dat, allnames)
    if (length(rhsnames)) {
        lhs = shallow(dat, lhsnames); rhs = shallow(dat, rhsnames); val = shallow(dat, valnames)
        # handle drop=TRUE/FALSE - Update: Logic moved to R, AND faster than previous version. Take that... old me :-).
        if (drop) {
            map = setDT(lapply(list(lhsnames, rhsnames), function(by) frankv(dat, by=by, ties.method="dense")))
            maporder = lapply(map, order_)
            mapunique = lapply(seq_along(map), function(i) .Call(CsubsetVector, map[[i]], maporder[[i]]))
            lhs = .Call(CsubsetDT, lhs, maporder[[1L]], seq_along(lhs))
            rhs = .Call(CsubsetDT, rhs, maporder[[2L]], seq_along(rhs))
        } else {
            lhs_ = cj_uniq(lhs); rhs_ = cj_uniq(rhs)
            map = vector("list", 2L)
            .Call(Csetlistelt, map, 1L, lhs_[lhs, which=TRUE])
            .Call(Csetlistelt, map, 2L, rhs_[rhs, which=TRUE])
            setDT(map)
            mapunique = vector("list", 2L)
            .Call(Csetlistelt, mapunique, 1L, seq_len(nrow(lhs_)))
            .Call(Csetlistelt, mapunique, 2L, seq_len(nrow(rhs_)))
            lhs = lhs_; rhs = rhs_
        }
        maplen = sapply(mapunique, length)
        idx = do.call("CJ", mapunique)[map, I := .I][["I"]] # TO DO: move this to C and avoid materialising the Cross Join.
        ans = .Call("Cfcast", lhs, val, maplen[[1L]], maplen[[2L]], idx, fill, fill.default, is.null(fun.call))
        allcols = do.call("paste", c(rhs, sep="_"))
        if (length(valnames) > 1L)
            allcols = do.call("paste", c(setcolorder(CJ(valnames, allcols, sorted=FALSE), 2:1), sep="_"))
        setattr(ans, 'names', c(lhsnames, allcols))
        setDT(ans); setattr(ans, 'sorted', lhsnames)
    } else {
        # formula is of the form x + y ~ . (rare case)
        if (drop) {
            if (is.null(subset) && is.null(fun.call)) {
                dat = copy(dat) # can't be avoided
                setkeyv(dat, lhsnames)
            }
            ans = dat
        } else {
            lhs = shallow(dat, lhsnames)
            val = shallow(dat, valnames)
            lhs_ = cj_uniq(lhs)
            idx = lhs_[lhs, I := .I][["I"]]
            lhs_[, I := NULL]
            ans = .Call("Cfcast", lhs_, val, nrow(lhs_), 1L, idx, fill, fill.default, is.null(fun.call))
            setDT(ans); setattr(ans, 'sorted', lhsnames)
            setnames(ans, c(lhsnames, valnames))
        }
        if (length(valnames) == 1L)
            setnames(ans, valnames, value.var)
    }
    return (ans)
}
