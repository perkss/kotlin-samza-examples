package demo

import com.google.common.collect.ImmutableMap
import demo.config.AppProperties
import demo.model.Order
import demo.serde.JacksonJsonSerde
import org.apache.samza.application.StreamApplication
import org.apache.samza.application.descriptors.StreamApplicationDescriptor
import org.apache.samza.context.Context
import org.apache.samza.operators.KV
import org.apache.samza.operators.MessageStream
import org.apache.samza.operators.OutputStream
import org.apache.samza.operators.functions.MapFunction
import org.apache.samza.serializers.KVSerde
import org.apache.samza.serializers.Serde
import org.apache.samza.serializers.StringSerde
import org.apache.samza.storage.SideInputsProcessor
import org.apache.samza.storage.kv.Entry
import org.apache.samza.storage.kv.KeyValueStore
import org.apache.samza.storage.kv.descriptors.RocksDbTableDescriptor
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor
import org.apache.samza.table.ReadWriteTable
import org.apache.samza.table.Table
import org.slf4j.LoggerFactory

class MyMapFunc : MapFunction<KV<String, Order>, KV<String, Order>> {

    companion object {
        private val logger = LoggerFactory.getLogger(MyMapFunc::class.java)
    }

    // TODO try other table like local
    private lateinit var profileTable: ReadWriteTable<String, Order>

    override fun init(context: Context) {
        profileTable = context.taskContext.getTable<String, Order>("order-table") as ReadWriteTable<String, Order>
    }

    override fun apply(message: KV<String, Order>): KV<String, Order> {
        val kv = KV.of(message.key, message)
        // profileTable.put(message.id, message)
        val savedMessage = profileTable.get(message.key)
        logger.info("Found saved message for {}, {}", message.key, savedMessage)

        return message
    }

}

class ProcessOrder : SideInputsProcessor {
    override fun process(message: IncomingMessageEnvelope?,
                         store: KeyValueStore<*, *>?): MutableCollection<Entry<String, Order>> {

        if (message == null) {
            println("No message")
            return mutableListOf()
        }
        println("Loaded Message ${message.message}")
        val order = message.message as Order
        return mutableListOf(Entry(order.id, order))
    }

}

class SamzaTopology(private val appProperties: AppProperties) : StreamApplication {

    private val KAFKA_SYSTEM_NAME = "kafka"
    private val KAFKA_CONSUMER_ZK_CONNECT: List<String> = listOf(appProperties.zookeeperServers)
    private val KAFKA_PRODUCER_BOOTSTRAP_SERVERS: List<String> = listOf(appProperties.bootstrapServers)
    private val KAFKA_DEFAULT_STREAM_CONFIGS: Map<String, String> = ImmutableMap.of("replication.factor", "3")

    // Match topic name
    private val INPUT_STREAM_ID = "order-request"
    private val OUTPUT_STREAM_ID = "grouped-orders"
    private val INVALID_USER_ID = "invalidUserId"

    override fun describe(appDescriptor: StreamApplicationDescriptor) {
        val kafkaSystemDescriptor = KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
                .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
                .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
                .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS)

        // TODO get this to work with key value as before
        val serde: KVSerde<String, Order> = KVSerde.of(StringSerde(), JacksonJsonSerde<Order>())

        // This doesnt consider jere its just assing
        val profileSerde: Serde<Order> = JacksonJsonSerde<Order>()
        val inputDescriptor: KafkaInputDescriptor<KV<String, Order>> = kafkaSystemDescriptor.getInputDescriptor<KV<String, Order>>(INPUT_STREAM_ID, serde)
        //  .shouldResetOffset()
        // .withOffsetDefault(SystemStreamMetadata.OffsetType.OLDEST)

        // Think about these serdes
        // TODO match this to changelog
        val orderProcessedCache = RocksDbTableDescriptor<String, Order>("order-table", serde)
        //  .withSideInputs(listOf(OUTPUT_STREAM_ID))
        //  .withSideInputsProcessor(ProcessOrder())


        val outputDescriptor: KafkaOutputDescriptor<KV<String, Order>> = kafkaSystemDescriptor.getOutputDescriptor<KV<String, Order>>(OUTPUT_STREAM_ID, serde)
        appDescriptor.withDefaultSystem(kafkaSystemDescriptor)

        val orders: MessageStream<KV<String, Order>> = appDescriptor.getInputStream<KV<String, Order>>(inputDescriptor)

        val filteredOrders: OutputStream<KV<String, Order>> = appDescriptor.getOutputStream<KV<String, Order>>(outputDescriptor)

        // TODO
        // https://samza.apache.org/learn/documentation/latest/jobs/samza-configurations.html
        // stores.store-name.changelog
        val table: Table<KV<String, Order>> = appDescriptor.getTable(orderProcessedCache)

        orders
                .map {
                    println("Order got $it")
                    // make fucntion process KV as signature
                    it
                }
                .map(MyMapFunc())
                // TODO next step write the message in the table.
                .sendTo(filteredOrders)
    }

}