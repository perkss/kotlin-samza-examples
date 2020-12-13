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

    // Tests done. Message not in Rocks Table. We check it in get. Then we put it and get it immediately and its still there
    override fun apply(message: KV<String, Order>): KV<String, Order> {
        val kv = KV.of(message.key, message)
        // To make sure it goes into the state store

        val savedMessage = profileTable.get(message.key)
        logger.info("Found saved message for {}, {}", message.key, savedMessage)
        // TODO this may not be required if side process it guarantee before next read.
        profileTable.put(message.key, message.value) // The side processor reads the write almost immediately
        val savedMessage2 = profileTable.get(message.key)
        logger.info("Updated saved message for {}, {}", message.key, savedMessage2)
        return message
    }

}

class ProcessOrder : SideInputsProcessor {

    companion object {
        private val logger = LoggerFactory.getLogger(ProcessOrder::class.java)
    }

    // TODO when I write to changelog this reads back automatically
    override fun process(message: IncomingMessageEnvelope?,
                         store: KeyValueStore<*, *>?): MutableCollection<Entry<String, Order>> {

        if (message == null) {
            logger.info("No message")
            return mutableListOf()
        }
        // TODO this bootstrap didnt seem to work?
        logger.info("Loaded Message {}, {}", message.key, message.message)
        val order = message.message as Order
        // TOOD format ID will make this work
        return mutableListOf(Entry(message.key as String, order))
    }

}

class SamzaTopology(private val appProperties: AppProperties) : StreamApplication {

    private val KAFKA_SYSTEM_NAME = "kafka"
    private val KAFKA_CONSUMER_ZK_CONNECT: List<String> = listOf(appProperties.zookeeperServers)
    private val KAFKA_PRODUCER_BOOTSTRAP_SERVERS: List<String> = listOf(appProperties.bootstrapServers, "localhost:9091", "localhost:9093")
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
        // .shouldResetOffset()
        // .withOffsetDefault(SystemStreamMetadata.OffsetType.OLDEST)

        // Think about these serdes
        // TODO match this to changelog
        val orderProcessedCache = RocksDbTableDescriptor<String, Order>("order-table", serde)
                .withSideInputs(listOf(OUTPUT_STREAM_ID))
                .withSideInputsProcessor(ProcessOrder())


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
                .sendTo(filteredOrders) // If this commented out and the put happens to rocks db it will not write it out still
        // it does not write it to the topic, only the table. SO its fine to write to Table above.
        // Loads from Changelog fine to bootstrap. -> test this
        // Also loads from table fine. Fine on restart even if not sent to changelog.
    }

}