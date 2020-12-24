package demo

import demo.config.AppProperties
import org.apache.samza.config.Config
import org.apache.samza.config.MapConfig
import org.apache.samza.runtime.LocalApplicationRunner
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication


@EnableConfigurationProperties(AppProperties::class)
@SpringBootApplication
class App(private val appProperties: AppProperties) : CommandLineRunner {

    companion object {
        private val logger = LoggerFactory.getLogger(App::class.java)
    }

    override fun run(vararg args: String) {
        logger.info("Running the Samza Application Examples")

        val samzaProperties = mapOf(
                "app.runner.class" to "org.apache.samza.runtime.LocalApplicationRunner",
                "samza.offset.default" to "oldest",
                "app.name" to "order-grouping-app",
                "app.id" to "2", // Baffled why docs say make this different. We need this to be the same to run fail over.
                "job.logged.store.base.dir" to "/Users/Stuart/Documents/Programming/kotlin/kotlin-samza-examples/2",
                // Checkpointing -> will protect against missed messages. Maintains the offset position to guarantee at least once semantics.
                "task.checkpoint.factory" to "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory",
                "stores.order-topology.default.stream.samza.key.serde" to "string",
                "stores.order-topology.default.stream.samza.msg.serde" to "json",
                "job.coordinator.factory" to "org.apache.samza.zk.ZkJobCoordinatorFactory",
                "job.coordinator.zk.connect" to "127.0.0.1:2181,127.0.0.1:22181,127.0.0.1:32181",
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
                "streams.grouped-orders.key.serde" to "string",
                "streams.grouped-orders.msg.serde" to "json",
        )
        val config: Config = MapConfig(samzaProperties)

        val zookeeperServers = appProperties.zookeeperServers.split(",")
        val bootstrapServers = appProperties.bootstrapServers.split(",")

        val runner = LocalApplicationRunner(OrderGroupingTopology(zookeeperServers, bootstrapServers), config)
        runner.run()
        runner.waitForFinish()
        logger.info("Passed wait till finish")

    }

}

fun main(args: Array<String>) {
    runApplication<App>(*args)
}