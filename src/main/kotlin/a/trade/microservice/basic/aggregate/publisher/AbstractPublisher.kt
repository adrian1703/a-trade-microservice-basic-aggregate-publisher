package a.trade.microservice.basic.aggregate.publisher

import a.trade.microservice.runtime_api.AsyncTaskManager
import a.trade.microservice.runtime_api.RuntimeApi
import org.slf4j.Logger
import org.springframework.context.Lifecycle
import java.util.concurrent.Callable

abstract class AbstractPublisher(
    protected val runtimeApi: RuntimeApi,
    protected val logger: Logger,
) : Lifecycle {

    private val asyncTaskManager = AsyncTaskManager(runtimeApi)

    override fun start() {
        logger.info("Starting")
        asyncTaskManager.task = taskToRun()
        asyncTaskManager.start()
    }

    override fun stop() {
        logger.info("Stopping")
        asyncTaskManager.stop()
    }

    override fun isRunning(): Boolean {
        val running = asyncTaskManager.isRunning()
        return running
    }

    protected abstract fun taskToRun(): Callable<*>
}