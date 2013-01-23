CREATE AGGREGATE array_accum (anyarray)
(
    sfunc = array_cat,
    stype = anyarray,
    initcond = '{}'
);
