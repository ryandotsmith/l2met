# testing l2met

There are not unit tests in l2met, only integration tests. The idea is to start l2met then run scripts that verify the input/output of l2met.

Tests assume you have [log-shuttle](https://github.com/ryandotsmith/log-shuttle) installed and available in `$PATH`.

A postgresql database must be available and running.

```bash
$ inintdb ./l2met-test
$ postgres -D ./l2met-test &
$ createdb metrics
$ psql metrics -f ./sql/schema.sql
```
