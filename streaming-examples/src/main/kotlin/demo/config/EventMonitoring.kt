package demo.config

import org.apache.samza.job.ApplicationStatus
import org.apache.samza.runtime.LocalApplicationRunner
import org.springframework.boot.actuate.audit.AuditEventRepository
import org.springframework.boot.actuate.audit.InMemoryAuditEventRepository
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.ReactiveHealthIndicator
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono

class SamzaHealthIndicator(private val localApplicationRunner: LocalApplicationRunner) : ReactiveHealthIndicator {

    override fun health(): Mono<Health> {
        val samzaState: ApplicationStatus = localApplicationRunner.status()
        return if (samzaState === ApplicationStatus.New || samzaState === ApplicationStatus.Running) {
            Health.up().withDetail("state", samzaState).build().toMono()
        } else Health.down().withDetail("state", samzaState).build().toMono()
    }
}

@Configuration
class EventMonitoring {

    @Bean
    fun samzaHealthIndicator(localApplicationRunner: LocalApplicationRunner): ReactiveHealthIndicator {
        return SamzaHealthIndicator(localApplicationRunner)
    }

    @Bean
    fun auditEventRepository(): AuditEventRepository {
        return InMemoryAuditEventRepository()
    }

}