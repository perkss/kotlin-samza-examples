package demo

import com.google.common.collect.ImmutableMap
import demo.model.Order
import demo.serde.JacksonJsonSerde
import org.apache.samza.application.StreamApplication
import org.apache.samza.application.descriptors.StreamApplicationDescriptor
import org.apache.samza.context.Context
import org.apache.samza.operators.KV
import org.apache.samza.operators.MessageStream
import org.apache.samza.operators.functions.MapFunction
import org.apache.samza.serializers.KVSerde
import org.apache.samza.serializers.StringSerde
import org.apache.samza.storage.kv.descriptors.RocksDbTableDescriptor
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor
import org.apache.samza.table.ReadWriteTable
import org.apache.samza.table.Table
import org.slf4j.LoggerFactory
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.primaryConstructor

inline infix fun <reified T : Any> T.merge(other: T): T {
    val propertiesByName = T::class.declaredMemberProperties.associateBy { it.name }
    val primaryConstructor = T::class.primaryConstructor
            ?: throw IllegalArgumentException("merge type must have a primary constructor")
    val args = primaryConstructor.parameters.associateWith { parameter ->
        val property = propertiesByName[parameter.name]
                ?: throw IllegalStateException("no declared member property found with name '${parameter.name}'")
        (property.get(this) ?: property.get(other))
    }
    return primaryConstructor.callBy(args)
}

class MergeOrder : MapFunction<KV<String, Order>, KV<String, Order>> {

    companion object {
        private val logger = LoggerFactory.getLogger(MergeOrder::class.java)
    }

    private lateinit var profileTable: ReadWriteTable<String, Order>

    override fun init(context: Context) {
        profileTable = context.taskContext.getTable<String, Order>("order-table") as ReadWriteTable<String, Order>
    }

    override fun apply(message: KV<String, Order>): KV<String, Order> {
        val savedMessage = profileTable.get(message.key)

        if (savedMessage != null) {
            logger.info("Found saved message for {}, {}", message.key, savedMessage)
            val mergedMessage = message.value merge savedMessage
            profileTable.put(message.key, mergedMessage)
        } else {
            profileTable.put(message.key, message.value)
        }

        val mergedSavedMessage = profileTable.get(message.key)
        logger.info("Updated saved message for {}, {}", message.key, mergedSavedMessage)
        return message
    }

}

class OrderGroupingTopology(private val zookeeperServers: List<String>,
                            private val bootstrapServers: List<String>) : StreamApplication {

    companion object {
        private val logger = LoggerFactory.getLogger(OrderGroupingTopology::class.java)
    }

    private val systemName = "order-topology"
    private val kafkaDefaultConfig: Map<String, String> = ImmutableMap.of("replication.factor", "3")

    // Match topic name
    private val orderRequestStreamId = "order-request"

    override fun describe(appDescriptor: StreamApplicationDescriptor) {
        val kafkaSystemDescriptor = KafkaSystemDescriptor(systemName)
                .withConsumerZkConnect(zookeeperServers)
                .withProducerBootstrapServers(bootstrapServers)
                .withDefaultStreamConfigs(kafkaDefaultConfig)

        val serde: KVSerde<String, Order> = KVSerde.of(StringSerde(), JacksonJsonSerde<Order>())

        val inputDescriptor: KafkaInputDescriptor<KV<String, Order>> = kafkaSystemDescriptor.getInputDescriptor<KV<String, Order>>(orderRequestStreamId, serde)

        val orderProcessedCache = RocksDbTableDescriptor<String, Order>("order-table", serde)

        appDescriptor.withDefaultSystem(kafkaSystemDescriptor)

        val table: Table<KV<String, Order>> = appDescriptor.getTable(orderProcessedCache)

        val orders: MessageStream<KV<String, Order>> = appDescriptor.getInputStream<KV<String, Order>>(inputDescriptor)

        orders
                .map {
                    logger.info("Order got {}", it)
                    it
                }
                .map(MergeOrder())
                .map {
                    logger.info("Order Processed and Merged {}", it)
                    it
                }
    }

}