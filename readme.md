## l2met

Convert structured log streams into Librato metrics.

```
2012-08-08T06:14:03+00:00 app[web.16]: app=shushu ns=utils measure=true fn=resources-hid-billable-events-eid elapsed=0.295
```

![img](http://f.cl.ly/items/0U1T1G082m3T0W2U4337/Screen%20Shot%202012-08-08%20at%209.06.42%20AM.png)

### Deploy to Heroku

* Create app with Ruby buildpack
* Configure Librato
* Attach route to app
* Point emitter app's at new wcld app

### Create App

```bash
$ git clone git://github.com/ryandotsmith/l2met.git
$ cd l2met
$ heroku create
$ git add . ; git commit -am "init"
$ git push heroku master
```

### Configure Librato

```bash
$ heroku config:add LIBRATO_EMAIL=you@domain.com
$ heroku config:add LIBRATO_TOKEN=abc123
```

### Attach Route

```bash
$ heroku routes:create
$ heroku routes:attach tcp://... receiver
```

### Start Receiver Process

```bash
$ heroku scale receiver=1 #singleton process
```

### Use it to drain an emitter app:

```bash
$ heroku drains:add syslog://... -a other-app
```
