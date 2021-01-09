package demo

import com.google.common.collect.ImmutableMap
import demo.model.Order
import demo.serde.JacksonJsonSerde
import org.apache.samza.application.TaskApplication
import org.apache.samza.application.descriptors.TaskApplicationDescriptor
import org.apache.samza.context.Context
import org.apache.samza.operators.KV
import org.apache.samza.serializers.KVSerde
import org.apache.samza.serializers.StringSerde
import org.apache.samza.storage.kv.descriptors.RocksDbTableDescriptor
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.system.SystemStream
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor
import org.apache.samza.table.ReadWriteTable
import org.apache.samza.task.*
import org.slf4j.LoggerFactory

class GroupOrder : StreamTask, InitableTask {

    companion object {
        private val logger = LoggerFactory.getLogger(GroupOrder::class.java)
    }

    private lateinit var profileTable: ReadWriteTable<String, Order>

    override fun init(context: Context) {
        profileTable = context.taskContext.getTable<String, Order>("order-table") as ReadWriteTable<String, Order>
    }

    override fun process(envelope: IncomingMessageEnvelope,
                         collector: MessageCollector,
                         coordinator: TaskCoordinator) {
        val message = envelope.message as Order

        val savedMessage = profileTable.get(message.id)

        if (savedMessage != null) {
            logger.info("Found saved message for {}, {}", message.id, savedMessage)
            val mergedMessage = message merge savedMessage
            profileTable.put(message.id, mergedMessage)
        } else {
            profileTable.put(message.id, message)
        }

        collector.send(OutgoingMessageEnvelope(SystemStream("order-topology", "grouped-orders"), message.id, message))

        coordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK)

        val savedMessage2 = profileTable.get(message.id)
        logger.info("Updated saved message for {}, {}", message.id, savedMessage2)
    }

}

class TaskFactory : StreamTaskFactory {
    override fun createInstance(): StreamTask {
        return GroupOrder()
    }

}

class OrderGroupingTaskApplication(private val zookeeperServers: List<String>,
                                   private val bootstrapServers: List<String>) : TaskApplication {

    companion object {
        private val logger = LoggerFactory.getLogger(OrderGroupingTopology::class.java)
    }

    private val systemName = "order-topology"
    private val kafkaDefaultSystemConfigs: Map<String, String> = ImmutableMap.of("replication.factor", "1")

    // Match topic name
    private val orderRequestStreamId = "order-request"
    private val groupedOrdersStreamId = "grouped-orders"

    override fun describe(appDescriptor: TaskApplicationDescriptor) {
        val kafkaSystemDescriptor = KafkaSystemDescriptor(systemName)
                .withConsumerZkConnect(zookeeperServers)
                .withProducerBootstrapServers(bootstrapServers)
                .withDefaultStreamConfigs(kafkaDefaultSystemConfigs)

        val serde: KVSerde<String, Order> = KVSerde.of(StringSerde(), JacksonJsonSerde<Order>())

        val inputDescriptor: KafkaInputDescriptor<KV<String, Order>> = kafkaSystemDescriptor.getInputDescriptor<KV<String, Order>>(orderRequestStreamId, serde)

        val orderProcessedCache = RocksDbTableDescriptor<String, Order>("order-table", serde)

        val outputDescriptor: KafkaOutputDescriptor<KV<String, Order>> = kafkaSystemDescriptor.getOutputDescriptor<KV<String, Order>>(groupedOrdersStreamId, serde)
        appDescriptor.withDefaultSystem(kafkaSystemDescriptor)

        appDescriptor
                .withInputStream(inputDescriptor)
                .withTable(orderProcessedCache)
                .withOutputStream(outputDescriptor)

        appDescriptor.withTaskFactory(TaskFactory())

    }

}