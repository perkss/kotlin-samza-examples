package demo

import demo.config.AppProperties
import org.apache.samza.runtime.LocalApplicationRunner
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication


@EnableConfigurationProperties(AppProperties::class)
@SpringBootApplication
class App(private val runner: LocalApplicationRunner) : CommandLineRunner {

    companion object {
        private val logger = LoggerFactory.getLogger(App::class.java)
    }

    override fun run(vararg args: String) {
        logger.info("Running the Samza Application Examples")
        runner.run()
        logger.info("Started Samza Application")
    }

}

fun main(args: Array<String>) {
    runApplication<App>(*args)
}