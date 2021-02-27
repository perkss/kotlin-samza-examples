package demo

import demo.config.AppProperties
import org.apache.samza.context.ExternalContext
import org.apache.samza.runtime.LocalApplicationRunner
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.actuate.audit.AuditEvent
import org.springframework.boot.actuate.audit.listener.AuditApplicationEvent
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.context.ApplicationEventPublisher

class AppExternalContext(val applicationEventPublisher: ApplicationEventPublisher) : ExternalContext

@EnableConfigurationProperties(AppProperties::class)
@SpringBootApplication
class App(
    private val runner: LocalApplicationRunner,
    private val applicationEventPublisher: ApplicationEventPublisher
) : CommandLineRunner {

    companion object {
        private val logger = LoggerFactory.getLogger(App::class.java)
    }

    override fun run(vararg args: String) {
        logger.info("Running the Samza Application Examples")
        applicationEventPublisher.publishEvent(AuditApplicationEvent(AuditEvent("SpringApp", "Started")))
        //Inject this and could log consumed first message or something in processor
        runner.run(AppExternalContext(applicationEventPublisher))
        applicationEventPublisher.publishEvent(
            AuditApplicationEvent(
                AuditEvent(
                    "Samza",
                    "Run Called with current state: ${runner.status()}"
                )
            )
        )
        logger.info("Started Samza Application")
    }

}

fun main(args: Array<String>) {
    runApplication<App>(*args)
}