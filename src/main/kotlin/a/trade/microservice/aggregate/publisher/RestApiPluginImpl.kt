package a.trade.microservice.aggregate.publisher

import a.trade.microservice.runtime_api.RestApiPlugin
import a.trade.microservice.runtime_api.RuntimeApi
import a.trade.microservice.runtime_api.Topics
import org.springframework.web.reactive.function.server.RouterFunction
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.router
import reactor.core.publisher.Mono

class RestApiPluginImpl : RestApiPlugin {

    private lateinit var runtimeApi: RuntimeApi

    override fun getRouter(): RouterFunction<ServerResponse> {
        val plugin = this
        return router {
            POST("/publish/baseData", plugin::publishBaseData)
            POST("/publish/single_1_min", plugin::publishSingle5minData)
            POST("/publish/all_1_day", plugin::publishAll1DayData)
        }
    }

    private fun publishBaseData(request: ServerRequest): Mono<ServerResponse> {
        BasicDataPublisher.getInstance(runtimeApi).start()
        return ServerResponse.ok().build()
    }

    private fun publishSingle5minData(request: ServerRequest): Mono<ServerResponse> {
        DerivedDataPublisher.getInstance(
            runtimeApi,
            Topics.Instance.STOCKAGGREGATE_ALL_1_MINUTE,
            Topics.Instance.STOCKAGGREGATE_SINGLE_1_MINUTE,
        ).start()
        return ServerResponse.ok().build()
    }

    private fun publishAll1DayData(request: ServerRequest): Mono<ServerResponse> {
        DerivedDataPublisher.getInstance(
            runtimeApi,
            Topics.Instance.STOCKAGGREGATE_ALL_1_MINUTE,
            Topics.Instance.STOCKAGGREGATE_ALL_1_DAY,
        ).start()
        return ServerResponse.ok().build()
    }

    override fun init(runtimeApi: RuntimeApi?) {
        this@RestApiPluginImpl.runtimeApi = runtimeApi!!
        // Smoke Test; eager init
        SmokeTest(runtimeApi).run()
//        runtimeApi.messageApi.clientSmokeTest()
    }
}