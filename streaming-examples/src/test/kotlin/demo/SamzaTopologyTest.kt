package demo

import demo.model.Order
import demo.serde.JacksonJsonSerde
import org.apache.samza.operators.KV
import org.apache.samza.serializers.KVSerde
import org.apache.samza.serializers.StringSerde
import org.apache.samza.test.framework.TestRunner
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor
import org.junit.jupiter.api.Test
import java.time.Duration

internal class SamzaTopologyTest {

    @Test
    fun `FLow test of our application`() {
        val samzaProperties = mapOf<String, String>(
                "serializers.registry.string.class" to "org.apache.samza.serializers.StringSerdeFactory",
                "serializers.registry.integer.class" to "org.apache.samza.serializers.IntegerSerdeFactory",
                "serializers.registry.json.class" to "demo.serde.JacksonJsonSerdeFactory",
                "stores.order-table.key.serde" to "string",
                "stores.order-table.msg.serde" to "json",
                "stores.order-table.changelog" to "grouped-orders",
                "task.commit.ms" to "500",

                )

        val inMemory = InMemorySystemDescriptor("test")

        val serde: KVSerde<String, Order> = KVSerde.of(StringSerde(), JacksonJsonSerde<Order>())

        val orderRequest = inMemory.getInputDescriptor("order-request", serde)

        val groupedOrdersStream = inMemory.getOutputDescriptor("grouped-orders", serde)

        val orderId = "1"
        val orderRequests = listOf(KV.of(orderId, Order(orderId, listOf("a"), 200)))

        val expectedGroupedOutput = listOf(KV.of(orderId, Order(orderId, listOf("a"), 200)))

        val orderGroupingTopology = SamzaTopology(listOf("localhost:2181"), listOf("localhost:9091"))
        TestRunner
                .of(orderGroupingTopology)
                .addConfig(samzaProperties)
                .addInputStream(orderRequest, orderRequests)
                .addOutputStream(groupedOrdersStream, 2)
                .run(Duration.ofMillis(10000))


        //    val result = TestRunner.consumeStream<KV<String, Order>>(groupedOrdersStream, Duration.ofMillis(1000))

        //  StreamAssert.containsInOrder(expectedGroupedOutput, groupedOrdersStream, Duration.ofMillis(1000))

    }

}