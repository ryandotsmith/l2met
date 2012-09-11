# l2met

## Usage

### Log Data Conventions

L2met uses certain heuristics to create metrics from log data.

#### Histogram

```
{measure: true, app: "myapp", fn: "your-fn-name", elapsed: 1.23}
```

#### Counter

```
{measure: true, app: "myapp", at: "your-code"}
```


#### Last Value

```
{measure: true, app: "myapp", at: "your-code", last: 99}
```

## Deploy to Heroku

Be sure and set config variables defined in lib/l2met/config.rb.

```bash
$ git clone git://github.com/ryandotsmith/l2met.git
$ cd l2met
$ heroku create
$ git add . ; git commit -am "init"
$ git push heroku master
$ heroku scale web=2 dboutlet=2
```
