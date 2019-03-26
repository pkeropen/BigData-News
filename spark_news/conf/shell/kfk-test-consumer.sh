#/bin/bash
echo "kfk-kafka-consumer.sh start......"
/bin/kafka-console-consumer --zookeeper hadoop01:2181 --from-beginning --topic webCount
