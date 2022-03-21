# EventBus 3
A reliable event/message hub for boosting Event-Driven architecture &amp; big data ingestion.

TODO:
x optimize configs structure
x optimize producer
x test http_app
x test producer
x test topic_resolver
x test signals on producer and topic_resolver 
- try k8s, confirm the deployment and config change strategies
x add producer into consumer
- complete app_consumer
- refactor thread exit mechanism
- producer update config
x consumer config changes, and config producer config changes
x if consumer downstream blocks, will the upstream be waiting? 
x producer log with caller id
- Sentry
- test consumer abnormal cases