package demo.examples

import demo.model.Order
import org.apache.samza.operators.KV
import org.apache.samza.serializers.NoOpSerde
import org.apache.samza.test.framework.TestRunner
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Duration

class TumblingWindowTest {

    @Test
    fun `Tumbling Window with early trigger on each item`() {
        val orders: List<Order> = listOf(Order("1", emptyList(), 0))

        val inMemorySystem = InMemorySystemDescriptor("kafka")

        val orderInputDescriptor: InMemoryInputDescriptor<KV<String, Order>> = inMemorySystem.getInputDescriptor<KV<String, Order>>("order-input", NoOpSerde<KV<String, Order>>())

        val orderCountOutputDescriptor: InMemoryOutputDescriptor<KV<String, Order>> = inMemorySystem.getOutputDescriptor<KV<String, Order>>("order-count-output", NoOpSerde<KV<String, Order>>())

        TestRunner
                .of(TumblingWindowExample())
                .addInputStream(orderInputDescriptor, orders)
                .addOutputStream(orderCountOutputDescriptor, 1)
                .run(Duration.ofSeconds(5))

        Assertions.assertTrue(TestRunner.consumeStream<Any>(orderCountOutputDescriptor, Duration.ofMillis(5000))[0]!!.size > 0)
    }

}