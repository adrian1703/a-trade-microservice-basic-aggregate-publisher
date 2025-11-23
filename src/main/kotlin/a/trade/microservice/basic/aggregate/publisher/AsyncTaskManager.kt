package a.trade.microservice.basic.aggregate.publisher

import a.trade.microservice.runtime_api.ExecutorContext
import a.trade.microservice.runtime_api.RuntimeApi
import net.jcip.annotations.ThreadSafe
import org.springframework.context.Lifecycle
import java.util.concurrent.Callable
import java.util.concurrent.Future
import kotlin.coroutines.cancellation.CancellationException



/**
 * Manages the execution of an asynchronous task in a thread-safe manner, allowing a task to be started, stopped,
 * and queried for its running status. The [start] method returns control to the caller immediately while starting
 * background execution.
 *
 * This class is intended to be used as a delegate for asynchronously executing a task and managing its lifecycle.
 *
 * @constructor Creates a new instance of AsyncTaskManager.
 * @param runtimeApi The runtime API used for accessing the executor service.
 *
 * Implements:
 * - [Lifecycle] to provide standardized start/stop lifecycle management.
 */
@ThreadSafe
class AsyncTaskManager(private val runtimeApi: RuntimeApi) : Lifecycle {
    private var isRunning = false
    private var taskFuture: Future<*>? = null
        @Synchronized get
    var task: Callable<*>? = null
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
