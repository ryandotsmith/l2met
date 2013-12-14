# An Important Update
L2met was born out of the desire to expose an effortless ingress point into Librato's systems. The project boasted no client libraries, 0-configuration (for the app developer), and percentile calculations. Since the inception of this project, the fine folks at Librato have always taken an interest in l2met. Librato has taken their interest to the extreme in that they now offer a hosted version of l2met. **You should be using their service for production metrics.** For instructions on how to integrate with Librato's log processing tier, see their [knowledge base article](http://support.metrics.librato.com/knowledgebase/articles/265391-heroku-native-and-custom-metrics-without-the-libra).

**This project is no longer maintained.** It will remain here as a historical artifact. If you have a use case that doesn't allow you to use Librato's hosted l2met, please open a support ticket with Librato.

Thanks for all of the contributions. I can take very little credit for the ideas that made l2met what it is today. Such OSS. Thanks to Heroku for providing me a home while I was developing this project. Finally, thanks to Librato for furthering the state of the art in metrics.

– Ryan Smith

# l2met

Turn these:
```ruby
$stdout.puts("measure#db.latency=4ms")
$stdout.puts("count#db.vaccum=1")
$stdout.puts("sample#db.size=100GB")
```

Into this:

![img](http://f.cl.ly/items/2R0h1x1b3V0Y0z0l1t1n/Screen%20Shot%202013-07-30%20at%209.59.52%20PM.png)


L2met receives HTTP requests that contain a body of RFC5424 formatted data. Commonly, data is drained into l2met by [logplex](https://github.com/heroku/logplex) or [log-shuttle](https://github.com/heroku/log-shuttle). Once data is delivered, l2met extracts and parses the log lines using the [logging conventions](https://github.com/ryandotsmith/l2met/wiki/Usage#log-conventions) and then stores the data in redis so that l2met outlets can read the data, build metrics, and deliver the metrics to your Librato account.

Checkout the wiki for information related to: [usage](https://github.com/ryandotsmith/l2met/wiki/Usage), [architecture](https://github.com/ryandotsmith/l2met/wiki/Architecture), and [administration](https://github.com/ryandotsmith/l2met/wiki/Administration).

## Getting Started

The easiest way to get l2met up and running is to deploy to Heroku. This guide assumes you have already created a Heroku & Librato account.

```bash
$ curl https://drone.io/github.com/ryandotsmith/l2met/files/l2met.tar.gz | tar xvz
$ ./scripts/setup my-l2met e@foo.com abc123
...
Drain URL: https://long-token@my-l2met.herokuapp.com/logs
```

This command will create Heroku app named *my-l2met* and return a drain URL with encrypted Librato credentials for a Librato account with email *e@foo.com* and an API token of *abc123*. After you have created *my-l2met*, you can add the drain URL to a Heroku app. A copy of the log stream will be delivered to *my-l2met* and metrics will be sent to the Librato account which your provided in the setup.

```bash
$ heroku drains:add https://long-token@my-l2met.herokuapp.com/logs -a myapp
```

## Hacking on l2met
L2met is an open source, community project. Patches are welcome. Open an issue prior to submitting a patch to ensure that your patch will be accepted. You will also receive tips & tricks on how to best implement your patch.

### Communcation

* [Mailing list](https://groups.google.com/d/forum/l2met)
* IRC - #l2met on freenode

### Documentation

* [GoDoc](http://godoc.org/github.com/ryandotsmith/l2met)
* [Wiki](https://github.com/ryandotsmith/l2met/wiki)

### Running Tests
[![Build Status](https://drone.io/github.com/ryandotsmith/l2met/status.png)](https://drone.io/github.com/ryandotsmith/l2met/latest)
```bash
$ ./redis-server --version
Redis server v=2.6.14 sha=f2f2b4eb:0 malloc=libc bits=64
$ go version
go version go1.1.1 darwin/amd64
```

```bash
$ git clone git://github.com/ryandotsmith/l2met.git $GOPATH/src/github.com/ryandotsmith/l2met
$ cd $GOPATH/src/github.com/ryandotsmith/l2met
$ export SECRETS=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | openssl base64)
$ export TZ=UTC
$ export REDIS_URL=redis://localhost:6379
$ ./redis-server &
$ go test ./...
```
