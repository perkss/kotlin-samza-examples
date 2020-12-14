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
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor
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

    // TODO try other table like local
    private lateinit var profileTable: ReadWriteTable<String, Order>

    override fun init(context: Context) {
        profileTable = context.taskContext.getTable<String, Order>("order-table") as ReadWriteTable<String, Order>
    }

    // Tests done. Message not in Rocks Table. We check it in get. Then we put it and get it immediately and its still there
    override fun apply(message: KV<String, Order>): KV<String, Order> {
        val kv = KV.of(message.key, message)
        // To make sure it goes into the state store
        val savedMessage = profileTable.get(message.key)

        val mergedMessage = message.value merge savedMessage

        logger.info("Found saved message for {}, {}", message.key, savedMessage)
        // TODO this may not be required if side process it guarantee before next read.
        profileTable.put(message.key, mergedMessage)
        val savedMessage2 = profileTable.get(message.key)
        logger.info("Updated saved message for {}, {}", message.key, savedMessage2)
        return message
    }

}

class SamzaTopology(private val zookeeperServers: List<String>,
                    private val bootstrapServers: List<String>) : StreamApplication {


    private val SYSTEM_NAME = "order-topology"
    private val KAFKA_DEFAULT_STREAM_CONFIGS: Map<String, String> = ImmutableMap.of("replication.factor", "3")

    // Match topic name
    private val INPUT_STREAM_ID = "order-request"
    private val OUTPUT_STREAM_ID = "grouped-orders"

    override fun describe(appDescriptor: StreamApplicationDescriptor) {
        val kafkaSystemDescriptor = KafkaSystemDescriptor(SYSTEM_NAME)
                .withConsumerZkConnect(zookeeperServers)
                .withProducerBootstrapServers(bootstrapServers)
                .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS)

        // TODO get this to work with key value as before
        val serde: KVSerde<String, Order> = KVSerde.of(StringSerde(), JacksonJsonSerde<Order>())

        val inputDescriptor: KafkaInputDescriptor<KV<String, Order>> = kafkaSystemDescriptor.getInputDescriptor<KV<String, Order>>(INPUT_STREAM_ID, serde)

        val orderProcessedCache = RocksDbTableDescriptor<String, Order>("order-table", serde)

        val outputDescriptor: KafkaOutputDescriptor<KV<String, Order>> = kafkaSystemDescriptor.getOutputDescriptor<KV<String, Order>>(OUTPUT_STREAM_ID, serde)
        appDescriptor.withDefaultSystem(kafkaSystemDescriptor)

        val table: Table<KV<String, Order>> = appDescriptor.getTable(orderProcessedCache)
        val orders: MessageStream<KV<String, Order>> = appDescriptor.getInputStream<KV<String, Order>>(inputDescriptor)

        orders
                .map {
                    println("Order got $it")
                    // make fucntion process KV as signature
                    it
                }
                .map(MergeOrder())
    }

}