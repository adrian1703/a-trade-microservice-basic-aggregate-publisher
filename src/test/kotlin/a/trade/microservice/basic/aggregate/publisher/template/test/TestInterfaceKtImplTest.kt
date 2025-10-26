package a.trade.microservice.basic.aggregate.publisher.test

import a.trade.microservice.runtime_api.test.TestInterface
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestInterfaceKtImplTest {
    @Test
    fun `test function should return Test`() {
        // Arrange
        val testInterfaceKtImpl: TestInterface = TestInterfaceKtImpl()

        // Act
        val result = testInterfaceKtImpl.test()

        // Assert
        assertEquals("Test", result, "The test function should return the expected string 'Test'.")
    }
}