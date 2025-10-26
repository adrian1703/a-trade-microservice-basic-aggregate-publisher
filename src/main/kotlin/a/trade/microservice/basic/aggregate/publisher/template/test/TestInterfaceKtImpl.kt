package a.trade.microservice.basic.aggregate.publisher.test

import a.trade.microservice.runtime_api.test.TestInterface

class TestInterfaceKtImpl: TestInterface {
    override fun test(): String {
        return "Test"
    }
}