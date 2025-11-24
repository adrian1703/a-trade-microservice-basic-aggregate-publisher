package a.trade.microservice.basic.aggregate.publisher

import a.trade.microservice.runtime_api.RestApiPlugin
import a.trade.microservice.runtime_api.RuntimeApi
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
            POST(
                "/publish/baseData", plugin::publishBaseData
            )
        }
    }

    private fun publishBaseData(request: ServerRequest): Mono<ServerResponse> {
        BasicDataPublisher.getInstance(runtimeApi).start()
        return ServerResponse.ok().build()
    }

    override fun init(runtimeApi: RuntimeApi?) {
        this@RestApiPluginImpl.runtimeApi = runtimeApi!!
        // Smoke Test; eager init
        SmokeTest(runtimeApi).run()
//        runtimeApi.messageApi.clientSmokeTest()
    }
}