import a.trade.microservice.runtime_api.RestApiPlugin
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File
import java.net.URLClassLoader
import java.util.ServiceLoader

class RestApiPluginLoadingTest {

//    @Test
//    fun `should discover RestApiPlugin implementation via ServiceLoader`() {
//        val loader = ServiceLoader.load(RestApiPlugin::class.java)
//        val implementations = loader.toList()
//        println(implementations.size)
//        assertTrue(
//            implementations.any { it::class.java.name == "a.trade.microservice.aggregate.publisher.RestApiPluginImpl" },
//            "RestApiPluginImplementation must be discoverable via ServiceLoader"
//        )
//    }
//
//    @Test
//    fun `should discover RestApiPlugin implementation from JAR file`() {
//        val jarFile = File("build/libs/a.trade.microservice.aggregate-publisher-0.0.1-plain.jar")
//        require(jarFile.exists()) { "JAR file not found: ${jarFile.absolutePath}" }
//        val url = jarFile.toURI().toURL()
//
//        // Use a dedicated classloader to load from just the plugin JAR
//        URLClassLoader(arrayOf(url), this::class.java.classLoader).use { classLoader ->
//            // Load the interface from the system classloader (so that it's the same type)
//            val serviceLoader = ServiceLoader.load(
//                RestApiPlugin::class.java,
//                classLoader
//            )
//            val implementations = serviceLoader.toList()
//            assertTrue(
//                implementations.any { it::class.java.name == "a.trade.microservice.aggregate.publisher.RestApiPluginImpl" },
//                "RestApiPluginImplementation must be discoverable via ServiceLoader from JAR"
//            )
//        }
//    }
}