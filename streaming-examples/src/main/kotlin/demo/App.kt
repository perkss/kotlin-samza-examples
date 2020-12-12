package demo

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
    }

}

fun main(args: Array<String>) {
    runApplication<App>(*args)
}