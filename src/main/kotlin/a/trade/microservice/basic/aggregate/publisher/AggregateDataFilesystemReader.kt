package a.trade.microservice.basic.aggregate.publisher

import com.opencsv.CSVReader
import kafka_message.StockAggregate
import net.jcip.annotations.GuardedBy
import net.jcip.annotations.ThreadSafe
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.BufferedInputStream
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.zip.GZIPInputStream

@ThreadSafe
class AggregateDataFilesystemReader(private val dataDirectory: String) {

    private val logger: Logger = LoggerFactory.getLogger(AggregateDataFilesystemReader::class.java)

    @GuardedBy("this")
    val dataFiles: ConcurrentLinkedQueue<File> = fetchDirectoryFiles()

    @Synchronized
    private fun fetchDirectoryFiles(): ConcurrentLinkedQueue<File> {
        val directory = File(dataDirectory)
        var result = directory.listFiles()?.toMutableList() ?: emptyList()
        result = result.filter { it.name.endsWith(".gz") }.sortedBy { it.name }
        transformBatch(result[0])
        println("done")
        return ConcurrentLinkedQueue(result)
    }

    @Synchronized
    fun hasNext(): Boolean {
        return dataFiles.isNotEmpty()
    }

    @Synchronized
    fun getBatchTaskTransformADay(): Callable<List<StockAggregate>> {
        val file = dataFiles.poll()
        assert(file != null)
        return Callable {
            try {
                return@Callable transformBatch(file)
            } catch (e: Exception) {
                e.printStackTrace()
                throw e
            }
        }
    }

    private fun transformBatch(dataFile: File): List<StockAggregate> {
        val aggregates = mutableListOf<StockAggregate>()
        logger.info("Processing file ${dataFile.name}")
        CSVReader(BufferedReader(InputStreamReader(GZIPInputStream(BufferedInputStream(dataFile.inputStream()))))).use { reader ->
            reader.readNext() // read header
            var row: Array<String>? = reader.readNext()
            while (row != null) {
                try {
                    val agg = StockAggregate(row[0],                   // ticker
                                             row[6].toLong(),          // window_start
                                             row[2].toDouble(),        // open
                                             row[4].toDouble(),        // high
                                             row[5].toDouble(),        // low
                                             row[3].toDouble(),        // close
                                             row[1].toLong()           // volume
                    )
                    aggregates.add(agg)
                    row = reader.readNext()
                } catch (e: Exception) {
                    logger.info("Error while processing row: $e")
                }
            }
        }
        return aggregates
    }
}