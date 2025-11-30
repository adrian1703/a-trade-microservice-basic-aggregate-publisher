package a.trade.microservice.aggregate.publisher

import kafka_message.StockAggregate
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.Callable

class AggregateDataFilesystemReaderTest {

    val dataDirectory = "data/minute_aggs_v1" // content root path

    @Test
    fun `should generate a list of callables`() {
        val reader = AggregateDataFilesystemReader(dataDirectory)
        val tasks = mutableListOf<Callable<List<StockAggregate>>>()
        while (reader.hasNext()) {
            tasks.add(reader.getBatchTaskTransformADay())
        }
        assertEquals(3, tasks.size, "The number of tasks should be 3.")
    }

    @Test
    fun `should produce agg objects`() {
        val reader = AggregateDataFilesystemReader(dataDirectory)
        val tasks = mutableListOf<Callable<List<StockAggregate>>>()
        tasks.add(reader.getBatchTaskTransformADay())
        val aggregates = tasks[0].call()
        println(aggregates.size)
        assertTrue(10000 < aggregates.size, "The number of tasks should be greater than 10k.")
    }
}