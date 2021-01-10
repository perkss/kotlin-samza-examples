# Samza Order Topology Example

```shell script
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:2181 --replication-factor 3 --partitions 3 --topic order-request

docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:2181 --replication-factor 3 --partitions 3 --topic grouped-orders --config cleanup.policy=compact

docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --zookeeper localhost:2181 --list
```


Produce
```shell script
docker exec -it kafka-1 kafka-console-producer --broker-list kafka-1:29091 --topic order-request --property "parse.key=true" --property "key.separator=:" 
docker exec -it kafka-1 kafka-console-producer --broker-list kafka-1:29091 --topic grouped-orders --property "parse.key=true" --property "key.separator=:" 
docker exec kafka-1 kafka-console-consumer --bootstrap-server kafka-1:29091 --topic grouped-orders --property print.key=true --property key.separator="-" --from-beginning
docker exec kafka-1 kafka-console-consumer --bootstrap-server kafka-1:29091 --topic __samza_coordinator_order-grouping_1 --property print.key=true --property key.separator="-" --from-beginning
docker exec kafka-1 kafka-console-consumer --bootstrap-server kafka-1:29091 --topic __samza_checkpoint_ver_1_for_order-grouping_1 --property print.key=true --property key.separator="-" --from-beginning
```

```shell script
5:{"id":"5", "products": ["2"]}
5:{"id":"5", "price":2}
1:{"id":"1", "price":2}
2:{"id":"2", "price":2}
4:{"id":"4", "price":2}





```