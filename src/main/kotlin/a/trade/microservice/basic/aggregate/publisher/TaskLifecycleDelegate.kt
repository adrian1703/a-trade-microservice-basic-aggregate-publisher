package a.trade.microservice.basic.aggregate.publisher

import a.trade.microservice.runtime_api.ExecutorContext
import a.trade.microservice.runtime_api.RuntimeApi
import net.jcip.annotations.ThreadSafe
import org.springframework.context.Lifecycle
import java.util.concurrent.Callable
import java.util.concurrent.Future
import kotlin.coroutines.cancellation.CancellationException

@ThreadSafe
class TaskLifecycleDelegate(private val runtimeApi: RuntimeApi) : Lifecycle {
    private var isRunning = false
    private var taskFuture: Future<*>? = null
        @Synchronized get
    private var task: Callable<*>? = null
        @Synchronized set

    @Synchronized
    override fun start() {
        if (!isRunning) {
            return
        }
        if (task == null) {
            throw Exception("Task is null.")
        }
        taskFuture = runtimeApi.getExecutorService(ExecutorContext.DEFAULT).submit(task)
    }

    @Synchronized
    override fun stop() {
        taskFuture?.cancel(true)
        try {
            taskFuture?.get()
        } catch (ignored: CancellationException) { // this is ok
        }
        isRunning = false
    }

    @Synchronized
    override fun isRunning(): Boolean {
        if (taskFuture != null) {
            if (taskFuture?.isDone == true || taskFuture?.isCancelled == true) {
                isRunning = false
            }
        }
        return isRunning
    }
}
