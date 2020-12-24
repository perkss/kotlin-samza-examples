package beam.demo

import demo.model.Order
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.slf4j.LoggerFactory

internal class PrintOrder : DoFn<Order, Order>() {

    companion object {
        private val logger = LoggerFactory.getLogger(PrintOrder::class.java)
    }

    @ProcessElement
    fun processElement(@Element wordLength: Order, out: OutputReceiver<Order>) {
        logger.info("Order is {}", wordLength)
        out.output(wordLength)
    }
}


class OrderTopology : PTransform<PCollection<Order>, PCollection<Order?>>() {

    override fun expand(input: PCollection<Order>): PCollection<Order?> {
        return input
                .apply(ParDo.of(PrintOrder()))
    }
}

