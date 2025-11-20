package a.trade.microservice.basic.aggregate.publisher

import a.trade.microservice.runtime_api.RuntimeApi
import org.springframework.context.Lifecycle
import java.util.concurrent.Callable

class BasicDataPublisher private constructor(
    runtimeApi: RuntimeApi,
    aggregateDataFilesystemReader: AggregateDataFilesystemReader = AggregateDataFilesystemReader("data/minute_aggs_v1"),
    ) : Lifecycle {

    init {
        if (!aggregateDataFilesystemReader.hasNext()) {
            throw IllegalArgumentException("No data found in the specified directory.")
        }
    }

    companion object {
        private var instance: BasicDataPublisher? = null

        @Synchronized
        fun getInstance(runtimeApi: RuntimeApi): BasicDataPublisher {
            if (instance == null) {
                instance = BasicDataPublisher(runtimeApi)
            }
            return instance!!
        }
    }
    private val taskLifecycleDelegate = TaskLifecycleDelegate(runtimeApi)

    override fun start() {
        taskLifecycleDelegate
        taskLifecycleDelegate.start()
    }

    override fun stop() {
        taskLifecycleDelegate.stop()
    }

    override fun isRunning(): Boolean {
        return taskLifecycleDelegate.isRunning()
    }

    private fun task(): Callable<*> {
        return Callable {

        }
    }
}