# EventBus v3

[![pipeline status](http://gitlab.thenetcircle.lab/tnc-service-team/eventbus3/badges/main/pipeline.svg)](http://gitlab.thenetcircle.lab/tnc-service-team/eventbus3/-/commits/main) 
[![test coverage](http://gitlab.thenetcircle.lab/tnc-service-team/eventbus3/badges/main/coverage.svg?job=coverage)](http://gitlab.thenetcircle.lab/tnc-service-team/eventbus3/badges/main/coverage.svg?job=coverage)

## Operation

### Resend the failed events

The events that sent failed to Sink will be forward to a dead-letter topic, I've created a script to resend any events from any topics.

``` sh
$ docker run --rm -it cloud-host-01.austria.private:5001/library/eventbus3:latest resend --help
usage: console_resend_event.py [-h] [-c CONFIG_FILE] -t TOPICS [TOPICS ...] -s SINK --start_time START_TIME [--headers HEADERS [HEADERS ...]] [--end_time END_TIME] [--wait_time WAIT_TIME]

EventBus v3 - Send Dead Letter Events

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG_FILE, --config_file CONFIG_FILE
                        Config file path. If not specified, it will look for environment variable `EB_CONF_FILE`
  -t TOPICS [TOPICS ...], --topics TOPICS [TOPICS ...]
                        The Kafka topics to consume
  -s SINK, --sink SINK  The http sink to send
  --start_time START_TIME
                        Start time in the format YYYY-MM-DDTHH:MM:SS
  --headers HEADERS [HEADERS ...]
                        The headers in the sink
  --end_time END_TIME   End time in the format YYYY-MM-DDTHH:MM:SS
  --wait_time WAIT_TIME
                        Max wait time in seconds (default: 10)
                        
                        
# Examples:
$ docker run --rm -it cloud-host-01.austria.private:5001/library/eventbus3:latest resend -c /app/configs/prod.yml -t event-v2-dead-letter-feti-vps event-v2-dead-letter-feti-payment-callback -s http://10.20.2.100:8002 --start_time "2024-05-07T11:39:00+0800" --end_time "2024-05-07T11:43:00+0800" --wait_time 3 --headers "Accept=application/json;version=1"

$ docker run --rm -it cloud-host-01.austria.private:5001/library/eventbus3:latest resend -c /app/configs/prod.yml -t event-v2-dead-letter-feti-dino event-v2-dead-letter-feti-messenger event-v2-dead-letter-feti-payment-callback event-v2-dead-letter-feti-queue event-v2-dead-letter-feti-vps -s http://10.20.14.4:8080/api.php/api/internal/eventbus/receiver --start_time "2024-05-07T11:24:00+0800" --end_time "2024-05-07T11:43:00+0800" --wait_time 5 --headers "Accept=application/json;version=1"
```

## Deploying

### Lab

Pushing changes to gitlab will run the tests and build the image, then deploy 
the docker image to `fat`.

### Staging

Running on `cloud-host-xx`. Deployment command:

```shell
docker pull cloud-host-01.austria.private:5001/library/my-service:latest
docker run -d -p 7760:8080 \
    -v /data/some-folder:/code/app/data \
    --name my-service-staging \
    --log-opt max-size=100m \
    --log-opt max-file=1 \
    cloud-host-01.austria.private:5001/library/my-service:latest
```

### Production

Running on `cloud-host-xx`. Deployment command:

```shell
docker pull cloud-host-01.austria.private:5001/library/my-service:latest
docker run -d -p 7770:8080 \
    -v /data/some-folder:/code/app/data \
    --name my-service-prod \
    --log-opt max-size=100m \
    --log-opt max-file=1 \
    --name my-service \
    cloud-host-01.austria.private:5001/library/my-service:latest
```

### Starting/stopping

Starting it if it's down:

```shell
docker restart quizmatch-service-fetiprod
```

### Logs

Use `--since` to get less logs. Pipe to `stdout` and paginate using `less`:

```shell
docker logs --since '2024-01-18T00:00:00' my-service-prod 2>&1 | less
```

## Features & Change Logs
A reliable event/message hub for boosting Event-Driven architecture &amp; big data ingestion.

TODO:
x optimize configs structure
x optimize producer
x test http_app
x test producer
x test topic_resolver
x test signals on producer and topic_resolver 
x remove old configs
x try k8s, confirm the deployment and config change strategies
x try app_consumer update config
x add producer into consumer 
x complete app_consumer
x refactor thread exit mechanism
x check producer error handler
x producer update config 
x consumer config changes, and config producer config changes
x if consumer downstream blocks, will the upstream be waiting? 
x producer log with caller id
- Sentry
x test consumer abnormal cases
x test consumer for new created topics
x try config map updates
x fix the config change function
  d split config files
  x make sure if that the config is invalid does not affect running status    
x k8s scripts
x after update producer config -- can not reproduce
```shell
Exception in callback <built-in method set_exception of _asyncio.Future object at 0x1067917c0>
handle: <Handle Future.set_exception>
Traceback (most recent call last):
  File "uvloop/cbhandles.pyx", line 63, in uvloop.loop.Handle._run
asyncio.exceptions.InvalidStateError: invalid state
Exception in callback <built-in method set_exception of _asyncio.Future object at 0x1067915c0>
handle: <Handle Future.set_exception>
Traceback (most recent call last):
  File "uvloop/cbhandles.pyx", line 63, in uvloop.loop.Handle._run
asyncio.exceptions.InvalidStateError: invalid state
Exception in callback <built-in method set_exception of _asyncio.Future object at 0x10679f840>
handle: <Handle Future.set_exception>
Traceback (most recent call last):
  File "uvloop/cbhandles.pyx", line 63, in uvloop.loop.Handle._run
asyncio.exceptions.InvalidStateError: invalid state
[2022-04-03 22:54:21 +0800] [74503] [INFO] Handling signal: winch
```
x check filemtime on kubenetes 
x refactor to use filemtime
x test changing the delay rebalance parameter on broker config
x improve config structure
x add consumer status (enable or disable)
x refactor commit offset to use store
x decrease the queues size
x test commit message instead of offsets
- continue to do the stress test
- change subscribed topics won't make a rebalance
x update producer borker the main_producer didn't restart
- log consumer start config
- test the consumer change topic but assigned old topic issue
- add a high offset protection code
- move new kafka from ch14,15,16 to gpu04,05,06
- test same consumer group subscribe to different topics

New:
- remove that too much info sending to Sentry
- when retry failed msg, don't send everything to Sentry

FOLLOWING PLAN:
x lab env
x staging and prod env
x new kafka cluster
- consume from backup cluster
- CI/CD
- monitoring
