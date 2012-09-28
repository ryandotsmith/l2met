# l2met

Convert your log stream into charts and actionable alerts in less than 1 minute
with 0 software installation.

## Usage

### Log Data Conventions

L2met uses heuristics to create metrics from log data. Ensure that you have the following style of logs:

#### Histogram

```
measure=true app=myapp fn="your-fn-name" elapsed=1.23
```

#### Counter

```
measure=true app=myapp at="something-important"
```

#### Last Value

```
measure=true app=myapp at="something-important" last=99
```

### Ruby

If you are using Ruby, you can use the scrolls gem to get structured log output.

```ruby
Scrolls.default_context(app: "myapp")

def test
  Scrolls.log(measure: true, fn: __method__) do
    sleep(1)
  end
end
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

## Arch

High level:

```
heroku app -> http log drains -> l2met -> librato
```

Inside of l2met:

```
l2met/web -> l2met/receiver -> l2met/register -> aws/dynamodb <- l2met/db-outlet -> librato/metrics
```
