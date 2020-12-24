package beam.demo

import demo.model.Order
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollection
import org.junit.jupiter.api.Test


internal class OrderTopologyTest {

    @Test
    fun orderTopologyTest() {
        val p: Pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false)

        val cr = p.coderRegistry
        cr.registerCoderForClass(Order::class.java, AvroCoder.of(Order::class.java))

        val input = p.apply(Create.of(Order("1", emptyList(), 1)))

        val output: PCollection<Order?> = input.apply(OrderTopology())

        val expected = listOf(Order("1", emptyList(), 1))

        PAssert.that(output).containsInAnyOrder(expected)

        p.run()
    }

}