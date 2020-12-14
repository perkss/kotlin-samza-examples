package demo.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "perkss.samza.example")
class AppProperties {
    lateinit var zookeeperServers: String
    lateinit var bootstrapServers: String
    lateinit var inputTopic: String
    lateinit var outputTopic: String
    lateinit var consumerGroupId: String
}