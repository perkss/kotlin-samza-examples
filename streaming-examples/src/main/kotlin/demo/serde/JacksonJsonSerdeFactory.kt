package demo.serde

import demo.model.Order
import org.apache.samza.config.Config
import org.apache.samza.serializers.Serde
import org.apache.samza.serializers.SerdeFactory

class JacksonJsonSerdeFactory : SerdeFactory<Order> {
    override fun getSerde(name: String?, config: Config?): Serde<Order> {
        return JacksonJsonSerde<Order>()
    }
}