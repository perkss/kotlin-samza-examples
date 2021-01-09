package demo

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource
import demo.model.Order
import junit.framework.Assert.assertEquals
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.codehaus.jackson.map.ObjectMapper
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.slf4j.LoggerFactory
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.support.TestPropertySourceUtils
import java.time.Duration

@SpringBootTest
@ContextConfiguration(initializers = [OrderGroupingAcceptanceTest.PropertyInit::class])
internal class OrderGroupingAcceptanceTest {

    companion object {
        private val logger = LoggerFactory.getLogger(OrderGroupingAcceptanceTest::class.java)

        @JvmField
        @RegisterExtension
        var sharedKafkaTestResource: SharedKafkaTestResource = SharedKafkaTestResource()
                .withBrokers(3)
    }


    @Test
    fun `Testing order grouping`() {
        logger.info("Zookeeper connect is {}", sharedKafkaTestResource.zookeeperConnectString)

        // TODO wait for Samza app to start
        // Expose samza app state over HTTP
        Thread.sleep(30000)
        val utils = sharedKafkaTestResource.kafkaTestUtils

        await atMost Duration.ofSeconds(60) untilAsserted {
            assertEquals("grouped-orders", utils.describeTopic("grouped-orders").name())
        }

        val objectMapper = ObjectMapper()
        val order = Order("1", listOf("ProductA"), null)

        val bytes = objectMapper.writeValueAsBytes(order)
        val key = objectMapper.writeValueAsBytes("1")

        utils.produceRecords(mapOf(key to bytes), "order-request", 0)

        var orderRequest: MutableList<ConsumerRecord<ByteArray, ByteArray>>
        await atMost Duration.ofSeconds(60) untilAsserted {
            orderRequest = utils.consumeAllRecordsFromTopic("order-request")
            assertEquals(1, orderRequest.size)
        }

        var groupedOrder = mutableListOf<ConsumerRecord<ByteArray, ByteArray>>()
        await atMost Duration.ofSeconds(60) untilAsserted {
            groupedOrder = utils.consumeAllRecordsFromTopic("grouped-orders")
            assertEquals(1, groupedOrder.size)
        }

        assertEquals("1", objectMapper.readValue(groupedOrder.first().key(), String::class.java))
        assertEquals(order, objectMapper.readValue(groupedOrder.first().value(), Order::class.java))

        val orderUpdate = Order("1", null, 2)

        val bytesOrderUpdate = objectMapper.writeValueAsBytes(orderUpdate)

        utils.produceRecords(mapOf(key to bytesOrderUpdate), "order-request", 0)

        var orderRequest2: MutableList<ConsumerRecord<ByteArray, ByteArray>>
        await atMost Duration.ofSeconds(60) untilAsserted {
            orderRequest2 = utils.consumeAllRecordsFromTopic("order-request")
            assertEquals(2, orderRequest2.size)
        }

        var mergedOrder = mutableListOf<ConsumerRecord<ByteArray, ByteArray>>()
        await atMost Duration.ofSeconds(60) untilAsserted {
            mergedOrder = utils.consumeAllRecordsFromTopic("grouped-orders")
            assertEquals(2, mergedOrder.size)
        }

        val expectedMergedOrder = Order("1", listOf("ProductA"), 2)

        assertEquals("1", objectMapper.readValue(mergedOrder[1].key(), String::class.java))
        assertEquals(expectedMergedOrder, objectMapper.readValue(mergedOrder[1].value(), Order::class.java))
    }

    class PropertyInit : ApplicationContextInitializer<ConfigurableApplicationContext> {
        override fun initialize(applicationContext: ConfigurableApplicationContext) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(applicationContext,
                    "perkss.samza.example.bootstrap-servers=${sharedKafkaTestResource.kafkaConnectString}",
                    "perkss.samza.example.zookeeper-servers=${sharedKafkaTestResource.zookeeperConnectString}"
            )
        }

    }

}