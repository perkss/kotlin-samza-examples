package beam.demo

import org.apache.samza.config.Config
import org.apache.samza.config.MapConfig
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class App : CommandLineRunner {

    companion object {
        private val logger = LoggerFactory.getLogger(App::class.java)
    }

    override fun run(vararg args: String) {
        logger.info("Running the Samza Application Examples")

        val samzaProperties = mapOf<String, String>(
                "samza.offset.default" to "oldest",
                "job.name" to "order-grouping",
                "job.coordinator.factory" to "org.apache.samza.zk.ZkJobCoordinatorFactory",
                "job.coordinator.zk.connect" to "localhost:2181",
                "job.default.system" to "order-topology",
                "task.name.grouper.factory" to "org.apache.samza.container.grouper.task.GroupByContainerIdsFactory",
                "task.drop.deserialization.errors" to "true",
                "serializers.registry.string.class" to "org.apache.samza.serializers.StringSerdeFactory",
                "serializers.registry.integer.class" to "org.apache.samza.serializers.IntegerSerdeFactory",
                "serializers.registry.json.class" to "demo.serde.JacksonJsonSerdeFactory",
                "stores.order-table.key.serde" to "string",
                "stores.order-table.msg.serde" to "json",
                "stores.order-table.changelog" to "grouped-orders",
                "stores.order-table.changelog.replication.factor" to "3",
                "task.commit.ms" to "600",
                "streams.grouped-orders.samza.offset.default" to "oldest", // changelog only read from start
                "streams.grouped-orders.samza.reset.offset" to "true" // changelog only read from start

        )
        val config: Config = MapConfig(samzaProperties)

//        val zookeeperServers = appProperties.zookeeperServers.split(",")
//        val bootstrapServers = appProperties.bootstrapServers.split(",")

        // val runner = LocalApplicationRunner(SamzaTopology(zookeeperServers, bootstrapServers), config)
        //  runner.run()

    }

}

fun main(args: Array<String>) {
    runApplication<App>(*args)
}