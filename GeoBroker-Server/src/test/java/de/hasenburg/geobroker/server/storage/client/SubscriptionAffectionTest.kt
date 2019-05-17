package de.hasenburg.geobroker.server.storage.client

import de.hasenburg.geobroker.commons.model.BrokerInfo
import org.apache.commons.lang3.tuple.ImmutablePair
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test

// constants
const val ip = "some random ip"
const val port = 1000

class SubscriptionAffectionTest {

    lateinit var subscriptionAffection: SubscriptionAffection

    // client identifier
    lateinit var cId1: String
    lateinit var cId2: String

    // subscription identifier
    lateinit var sub1_1: ImmutablePair<String, Int>
    lateinit var sub1_2: ImmutablePair<String, Int>
    lateinit var sub1_3: ImmutablePair<String, Int>
    lateinit var sub2_1: ImmutablePair<String, Int>
    lateinit var sub2_2: ImmutablePair<String, Int>

    // broker infos
    lateinit var bi1: BrokerInfo
    lateinit var bi2: BrokerInfo
    lateinit var bi3: BrokerInfo

    @Before
    fun setUp() {
        subscriptionAffection = SubscriptionAffection()
        cId1 = "Client 1"
        cId2 = "Client 2"

        sub1_1 = ImmutablePair(cId1, 1)
        sub1_2 = ImmutablePair(cId1, 2)
        sub1_3 = ImmutablePair(cId1, 3)

        sub2_1 = ImmutablePair(cId2, 1)
        sub2_2 = ImmutablePair(cId2, 2)

        bi1 = BrokerInfo("Broker 1", ip, port)
        bi2 = BrokerInfo("Broker 2", ip, port)
        bi3 = BrokerInfo("Broker 3", ip, port)
    }

    @After
    fun tearDown() {
    }

    @Test
    fun testAffections() {
        // test init
        assertEquals(0, subscriptionAffection.updateAffections(sub1_1, listOf(bi1, bi2)).size)
        assertEquals(0, subscriptionAffection.updateAffections(sub1_2, listOf(bi3)).size)
        assertEquals(0, subscriptionAffection.updateAffections(sub1_3, listOf()).size)

        assertEquals(0, subscriptionAffection.updateAffections(sub2_1, listOf(bi1, bi2)).size)
        assertEquals(0, subscriptionAffection.updateAffections(sub2_2, listOf(bi2)).size)

        // test get
        assertEquals(setOf(bi1, bi2, bi3), subscriptionAffection.getAffections(cId1))
        assertEquals(setOf(bi1, bi2), subscriptionAffection.getAffections((cId2)))

        // overwrite
        assertEquals(listOf(bi1), subscriptionAffection.updateAffections(sub1_1, listOf(bi2, bi3)))
        assertEquals(emptyList<BrokerInfo>(), subscriptionAffection.updateAffections(sub1_2, listOf(bi3)))
        assertEquals(emptyList<BrokerInfo>(), subscriptionAffection.updateAffections(sub1_3, listOf(bi2)))

        // test get
        assertEquals(setOf(bi2, bi3), subscriptionAffection.getAffections(cId1))
        assertEquals(setOf(bi1, bi2), subscriptionAffection.getAffections((cId2)))
    }
}