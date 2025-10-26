package a.trade.microservice.template.test

import com.fasterxml.jackson.annotation.JsonProperty
import io.swagger.v3.oas.annotations.media.Schema

/**
 *
 * @param testprop1
 * @param testprop2 Desc
 */
data class TestComponent(

    @Schema(example = "null", description = "")
    @get:JsonProperty("testprop1") val testprop1: Int? = null,

    @Schema(example = "null", description = "Desc")
    @get:JsonProperty("testprop2") val testprop2: String? = null
) {

}

