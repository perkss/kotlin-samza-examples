import ch.qos.logback.classic.encoder.PatternLayoutEncoder

appender("CONSOLE", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "%d{HH:mm:ss.SSS} %-4relative [%thread] - %msg%n"
    }
}
root(INFO, ["CONSOLE"])