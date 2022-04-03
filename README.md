# EventBus 3
A reliable event/message hub for boosting Event-Driven architecture &amp; big data ingestion.

TODO:
x optimize configs structure
x optimize producer
x test http_app
x test producer
x test topic_resolver
x test signals on producer and topic_resolver 
x remove old configs
- try k8s, confirm the deployment and config change strategies
- try app_consumer update config
x add producer into consumer 
x complete app_consumer
x refactor thread exit mechanism
x check producer error handler
- producer update config 
x consumer config changes, and config producer config changes
x if consumer downstream blocks, will the upstream be waiting? 
x producer log with caller id
- Sentry
- test consumer abnormal cases

x try config map updates
- fix the config change function
  - split config files
  - make sure if that the config is invalid does not affect running status    
- k8s scripts