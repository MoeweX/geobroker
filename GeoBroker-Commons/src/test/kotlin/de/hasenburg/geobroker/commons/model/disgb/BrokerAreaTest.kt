package de.hasenburg.geobroker.commons.model.disgb

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import org.apache.logging.log4j.LogManager
import org.junit.Assert
import org.junit.Test

private val logger = LogManager.getLogger()

class BrokerAreaTest {

    @Test
    fun testSerialize() {
        val brokerInfo = BrokerInfo("brokerId", "address", 1000)
        val brokerArea1 = BrokerArea(brokerInfo, Geofence.circle(Location.random(), 10.0))

        val json = brokerArea1.toJson()
        logger.info(json)
        val brokerArea2: BrokerArea = json.toBrokerArea()
        Assert.assertEquals(brokerArea1, brokerArea2)
    }

    @Test
    fun testContainsLocation() {
        val brokerInfo = BrokerInfo("brokerId", "address", 1000)
        val `in` = Location(10.0, 10.0)
        val out = Location(30.0, 30.0)
        val brokerArea = BrokerArea(brokerInfo, Geofence.circle(`in`, 10.0))
        Assert.assertTrue(brokerArea.containsLocation(`in`))
        Assert.assertFalse(brokerArea.containsLocation(out))
    }

    @Test
    fun testResponsibleBroker() {
        val ownId = "repBroker"
        val responsibleBroker = BrokerInfo(ownId, "address", 1000)
        val otherBroker = BrokerInfo("otherBroker", "address", 1000)
        val brokerArea = BrokerArea(responsibleBroker,
                Geofence.circle(Location.random(), 10.0))
        // check broker info
        Assert.assertEquals(responsibleBroker, brokerArea.responsibleBroker)
        Assert.assertNotEquals(otherBroker, brokerArea.responsibleBroker)
        // use check method
        Assert.assertTrue(brokerArea.hasResponsibleBroker(responsibleBroker.brokerId))
        Assert.assertFalse(brokerArea.hasResponsibleBroker(otherBroker.brokerId))
    }

    @Test
    fun testWorldResponsibility() {
        val brokerInfo = BrokerInfo("brokerId", "address", 1000)
        val brokerArea = BrokerArea(brokerInfo, Geofence.world())
        logger.info(brokerArea)
        for (i in 0..99) {
            Assert.assertTrue(brokerArea.containsLocation(Location.random()))
        }
    }

}