package demo.examples

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import demo.model.Order
import org.apache.samza.application.StreamApplication
import org.apache.samza.application.descriptors.StreamApplicationDescriptor
import org.apache.samza.operators.KV
import org.apache.samza.operators.MessageStream
import org.apache.samza.operators.OutputStream
import org.apache.samza.operators.functions.FoldLeftFunction
import org.apache.samza.operators.triggers.Triggers
import org.apache.samza.operators.windows.Windows
import org.apache.samza.serializers.IntegerSerde
import org.apache.samza.serializers.JsonSerdeV2
import org.apache.samza.serializers.KVSerde
import org.apache.samza.serializers.StringSerde
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor
import java.io.Serializable
import java.time.Duration

class TumblingWindowExample : StreamApplication, Serializable {

    companion object {
        private const val KAFKA_SYSTEM_NAME = "kafka"
        private val KAFKA_CONSUMER_ZK_CONNECT: List<String> = ImmutableList.of("localhost:2181")
        private val KAFKA_PRODUCER_BOOTSTRAP_SERVERS: List<String> = ImmutableList.of("localhost:9092")
        private val KAFKA_DEFAULT_STREAM_CONFIGS: Map<String, String> = ImmutableMap.of("replication.factor", "1")
        private const val INPUT_STREAM_ID = "order-input"
        private const val OUTPUT_STREAM_ID = "order-count-output"
    }

    override fun describe(appDescriptor: StreamApplicationDescriptor) {

        val kafkaSystemDescriptor = KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
                .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
                .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
                .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS)

        val orderSerde: KVSerde<String, Order> = KVSerde.of(StringSerde(), JsonSerdeV2<Order>(Order::class.java))
        val orderCountSerde: KVSerde<String, Int> = KVSerde.of(StringSerde(), IntegerSerde())
        val orderInputDescriptor: KafkaInputDescriptor<KV<String, Order>> = kafkaSystemDescriptor.getInputDescriptor<KV<String, Order>>(INPUT_STREAM_ID, orderSerde)

        val orderCountOutputDescriptor: KafkaOutputDescriptor<KV<String, Int>> = kafkaSystemDescriptor.getOutputDescriptor<KV<String, Int>>(OUTPUT_STREAM_ID, orderCountSerde)
        appDescriptor.withDefaultSystem(kafkaSystemDescriptor)
        val orderViews: MessageStream<KV<String, Order>> = appDescriptor.getInputStream<KV<String, Order>>(orderInputDescriptor)
        val orderCountStream: OutputStream<KV<String, Int>> = appDescriptor.getOutputStream<KV<String, Int>>(orderCountOutputDescriptor)

        val aggregateFunction = FoldLeftFunction { msg: KV<String, Order>, oldValue: Int -> oldValue.inc() }

        orderViews
                .window(
                        Windows.keyedTumblingWindow(
                                { it.key },
                                Duration.ofSeconds(1),
                                { 0 },
                                aggregateFunction,
                                StringSerde(),
                                IntegerSerde())
                                .setEarlyTrigger(Triggers.count(1)),
                        "count"
                )
                .map {
                    KV.of(it.key.key, it.message)
                }
                .sendTo(orderCountStream)
    }
}