# Apache Kafka playground

* _AlterTopicConfigs_ : how to get and modify/updated a topic configuration using the Admin Client API;
* _AssignedSubscribedConsumers_ : it shows how a consumer asking to be assigning a specific partition for a topic (using _assign()_ method)  without
being part of any consumer group can live together with a consumer which subscribes to same topic (using _subscribe()_ method) and is part of a consumer group.
The common partition (manually and automatically assigned) is available to both for reading messages;
* _CreateTopic_ : it shows a really simple way to create a topic with default configuration;
* _AddRemoveSubscriptions_ : it shows that subscribe operation isn't incremental. You have to provide the full list of topics to subscribe every time you call _subscribe()_ even with previous topics;
* _MaxConnectionsPerIP_ : it shows how the "max.connections.per.ip" broker property influences a single client (consumer/producer) as well;
* _AlterTopicPartitions_: it shows how it's possible to increase the number of partitions for a topic;
* _ProducerNotExistingTopic_: it shows hot to catch error about sending to a non existing topic using interceptor;
* _ConsumerGroupOnTopics_ : it shows the difference between default RangeAssignor and the RoundRobinAssignor which allows to rebalance partitions belonging different topics across different consumers in the same consumer group;
* _DescribeQuorumMetadata_: it shows how discribing the cluster in terms of brokers differs from the outcome with the the quorum info in terms of voters and observers;