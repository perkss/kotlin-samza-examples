package demo

import demo.model.Order
import org.apache.samza.operators.KV
import org.apache.samza.serializers.NoOpSerde
import org.apache.samza.test.framework.TestRunner
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.time.Duration

@Disabled
internal class OrderGroupingTopologyTest {

    //https://github.com/apache/samza/blob/master/samza-test/src/test/java/org/apache/samza/test/table/TestLocalTableWithSideInputsEndToEnd.java#L59
    @Test
    fun `Flow test of our application`() {
        val samzaProperties = mapOf<String, String>(
                "serializers.registry.string.class" to "org.apache.samza.serializers.StringSerdeFactory",
                "serializers.registry.integer.class" to "org.apache.samza.serializers.IntegerSerdeFactory",
                "serializers.registry.json.class" to "demo.serde.JacksonJsonSerdeFactory",
                "stores.order-table.key.serde" to "string",
                "stores.order-table.msg.serde" to "json",
                "stores.order-table.changelog" to "grouped-orders",
                "task.commit.ms" to "10",
                "serializers.registry.string.class" to "org.apache.samza.serializers.StringSerdeFactory",
                "serializers.registry.integer.class" to "org.apache.samza.serializers.IntegerSerdeFactory",
                "serializers.registry.json.class" to "demo.serde.JacksonJsonSerdeFactory",
                "stores.order-table.key.serde" to "string",
                "stores.order-table.msg.serde" to "json",
                "stores.order-table.changelog" to "grouped-orders",
                "stores.order-table.changelog.replication.factor" to "2",
                "stores.order-table.factory" to "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory")

        // Note: Has to match the system name defined in Samza Application
        val inMemory = InMemorySystemDescriptor("order-topology")

        val serde = NoOpSerde<Any>()

        val orderRequest = inMemory.getInputDescriptor("order-request", serde)

        val groupedOrdersStream = inMemory.getOutputDescriptor("grouped-orders", serde)

        val orderId = "1"
        val orderRequests = listOf(KV.of(orderId, Order(orderId, listOf("a"), 200)))

        val expectedGroupedOutput = listOf(Order(orderId, listOf("a"), 200))

        val orderGroupingTopology = OrderGroupingTopology(listOf("localhost:2181"), listOf("localhost:9091"))
        TestRunner
                .of(orderGroupingTopology)
                .addConfig(samzaProperties)
                .addInputStream(orderRequest, orderRequests)
                .addOutputStream(groupedOrdersStream, 1)
                .run(Duration.ofSeconds(2))

        // TODO can assert on storage directory
        //   StreamAssert.containsInOrder(expectedGroupedOutput, groupedOrdersStream, Duration.ofSeconds(10))

    }

}