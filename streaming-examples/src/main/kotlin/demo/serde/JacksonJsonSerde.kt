package demo.serde

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import demo.model.Order
import org.apache.samza.serializers.Serde

class JacksonJsonSerde<T> : Serde<Order> {

    override fun toBytes(order: Order): ByteArray {
        // TODO didnt work as field due to serialization of it by samza look at there JSON SERDE
        return ObjectMapper().apply {
            registerModule(KotlinModule())
            registerModule(JavaTimeModule())
        }.writeValueAsBytes(order)
    }

    override fun fromBytes(bytes: ByteArray): Order {
        return ObjectMapper().apply {
            registerModule(KotlinModule())
            registerModule(JavaTimeModule())
        }.readValue(bytes, Order::class.java)
    }
}