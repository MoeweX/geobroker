package de.hasenburg.geobroker.commons.model.disgb

import org.apache.logging.log4j.LogManager
import org.junit.Assert
import org.junit.Test
import java.lang.Exception

private val logger = LogManager.getLogger()

class BrokerInfoTest {

    @Test
    fun testSerialize() {
        val brokerInfo1 = BrokerInfo("brokerId", "address", 1000)
        val json1 = brokerInfo1.toJson()
        logger.info(json1)
        val brokerInfo2 = json1.toBrokerInfo()
        Assert.assertEquals(brokerInfo1, brokerInfo2)
    }

    @Test(expected = Exception::class)
    fun testFailedSerialize() {
        val brokerInfo = BrokerInfo("brokerId", "address", 1000)
        val json = brokerInfo.toJson()
        val wrong = json.toBrokerArea()
    }

}