package a.trade.microservice.aggregate.publisher

import a.trade.microservice.runtime_api.RuntimeApi

class SmokeTest(val runtimeApi: RuntimeApi) : Runnable {
    override fun run() {
        BasicDataPublisher.getInstance(runtimeApi) // should initialize and do its own sanity check
    }
}