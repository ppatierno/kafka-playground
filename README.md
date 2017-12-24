# Apache Kafka playground

* _AlterTopicConfigs_ : how to get and modify/updated a topic configuration using the Admin Client API;
* _AssignedSubscribedConsumers_ : it shows how a consumer asking to be assigning a specific partition for a topic (using _assign()_ method)  without
being part of any consumer group can live together with a consumer which subscribes to same topic (using _subscribe()_ method) and is part of a consumer group.
The common partition (manually and automatically assigned) is available to both for reading messages;
* _CreateTopic_ : it shows a really simple way to create a topic with default configuration;
* _AddRemoveSubscriptions_ : it shows that subscribe operation isn't incremental. You have to provide the full list of topics to subscribe every time you call _subscribe()_ even with previous topics;