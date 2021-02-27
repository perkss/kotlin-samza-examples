package demo.config

import org.apache.samza.job.ApplicationStatus
import org.apache.samza.runtime.LocalApplicationRunner
import org.codehaus.jackson.map.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.boot.actuate.audit.AuditEventRepository
import org.springframework.boot.actuate.audit.InMemoryAuditEventRepository
import org.springframework.boot.actuate.endpoint.SecurityContext
import org.springframework.boot.actuate.endpoint.http.ApiVersion
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse
import org.springframework.boot.actuate.endpoint.web.annotation.EndpointWebExtension
import org.springframework.boot.actuate.health.*
import org.springframework.boot.actuate.health.Status.UP
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono

@Component
@EndpointWebExtension(endpoint = HealthEndpoint::class)
class LoggingReactiveHealthEndpointWebExtension(
    registry: ReactiveHealthContributorRegistry,
    groups: HealthEndpointGroups
) : ReactiveHealthEndpointWebExtension(registry, groups) {

    companion object {
        private val logger = LoggerFactory.getLogger(LoggingReactiveHealthEndpointWebExtension::class.java)
    }

    override fun health(
        apiVersion: ApiVersion?,
        securityContext: SecurityContext?,
        showAll: Boolean,
        vararg path: String?
    ): Mono<WebEndpointResponse<out HealthComponent>> {
        val health = super.health(apiVersion, securityContext, showAll, *path)

        return health.doOnNext {
            if (it.body.status == UP) {
                logger.info("Health status: {}, {}", it.body.status, ObjectMapper().writeValueAsString(it.body))
            }
        }
    }
}

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