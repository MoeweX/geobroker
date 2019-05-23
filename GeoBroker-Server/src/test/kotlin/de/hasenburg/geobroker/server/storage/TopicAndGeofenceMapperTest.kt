package de.hasenburg.geobroker.server.storage

import de.hasenburg.geobroker.commons.model.message.Topic
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.randomInt
import de.hasenburg.geobroker.commons.randomName
import de.hasenburg.geobroker.server.main.Configuration
import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.logging.log4j.LogManager
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.*
import java.util.stream.Collectors

private val logger = LogManager.getLogger()

class TopicAndGeofenceMapperTest {

    private lateinit var mapper: TopicAndGeofenceMapper

    @Test
    fun testGetMatchingTopicLevels_NoWildcards() {
        mapper = TopicAndGeofenceMapper(Configuration())
        mapper.anchor.getOrCreateChildren("a", "b", "c")
        mapper.anchor.getOrCreateChildren("a", "b", "d")
        checkTopicLevels(arrayOf("c"), mapper.getMatchingTopicLevels(Topic("a/b/c")))
        checkTopicLevels(arrayOf("b"), mapper.getMatchingTopicLevels(Topic("a/b")))
        checkTopicLevels(arrayOf(), mapper.getMatchingTopicLevels(Topic("b")))
    }

    @Test
    fun testGetMatchingTopicLevels_SingleLevelWildcards() {
        mapper = TopicAndGeofenceMapper(Configuration())
        mapper.anchor.getOrCreateChildren("a", "b", "c")
        mapper.anchor.getOrCreateChildren("a", "b", "+")
        mapper.anchor.getOrCreateChildren("a", "+", "c")
        checkTopicLevels(arrayOf("c", "+", "c"), mapper.getMatchingTopicLevels(Topic("a/b/c")))
        checkTopicLevels(arrayOf("+"), mapper.getMatchingTopicLevels(Topic("a/b/x")))
        checkTopicLevels(arrayOf("+"), mapper.getMatchingTopicLevels(Topic("a/x")))
    }

    @Test
    fun testGetMatchingTopicLevels_MultiLevelWildcards() {
        mapper = TopicAndGeofenceMapper(Configuration())
        mapper.anchor.getOrCreateChildren("a", "b", "c")
        mapper.anchor.getOrCreateChildren("a", "b", "+")
        mapper.anchor.getOrCreateChildren("a", "+", "c")
        mapper.anchor.getOrCreateChildren("a", "#")
        mapper.anchor.getOrCreateChildren("#")
        checkTopicLevels(arrayOf("c", "+", "c", "#", "#"), mapper.getMatchingTopicLevels(Topic("a/b/c")))
        checkTopicLevels(arrayOf("+", "#", "#"), mapper.getMatchingTopicLevels(Topic("a/b/x")))
        checkTopicLevels(arrayOf("+", "#", "#"), mapper.getMatchingTopicLevels(Topic("a/x")))
        // subscription a/# should produce a match for a, and #
        checkTopicLevels(arrayOf("#", "a", "#"), mapper.getMatchingTopicLevels(Topic("a")))

        checkTopicLevels(arrayOf("#"), mapper.getMatchingTopicLevels(Topic("b")))
        checkTopicLevels(arrayOf("#", "#"), mapper.getMatchingTopicLevels(Topic("a/b/c/d")))
    }

    @Test
    fun testOneGeofenceOneTopic() {
        mapper = TopicAndGeofenceMapper(Configuration())

        // prepare
        val testIds = testIds(randomInt(20), randomInt(100))
        logger.info("Generated {} testing ids", testIds.size)
        val t = Topic("test/topic")

        // test berlin
        testIds.forEach { id -> mapper.putSubscriptionId(id, t, berlinRectangle()) }
        var returnedIds = mapper.getSubscriptionIds(t, berlinPoint()).toMutableSet()

        // verify berlin
        assertEquals(testIds.size.toLong(), returnedIds.size.toLong())
        returnedIds.removeAll(testIds)
        assertTrue(returnedIds.isEmpty())

        // test hamburg
        returnedIds = mapper.getSubscriptionIds(t, hamburgPoint()).toMutableSet()

        // verify hamburg
        assertEquals(0, returnedIds.size.toLong())
    }

    @Test
    fun testOneGeofenceManyTopics() {
        mapper = TopicAndGeofenceMapper(Configuration())

        // prepare
        val testIds1 = testIds(randomInt(20), randomInt(100))
        val testIds2 = testIds(randomInt(20), randomInt(100))
        logger.info("Generated {} and {} testing ids", testIds1.size, testIds2.size)
        val t1 = Topic("t/1")
        val t2 = Topic("t/+/2")

        // test berlin
        testIds1.forEach { id -> mapper.putSubscriptionId(id, t1, berlinRectangle()) }
        testIds2.forEach { id -> mapper.putSubscriptionId(id, t2, berlinRectangle()) }

        var returnedIds1: MutableSet<ImmutablePair<String, Int>> = mapper.getSubscriptionIds(t1, berlinPoint())
                .toMutableSet()
        var returnedIds2: MutableSet<ImmutablePair<String, Int>> = mapper.getSubscriptionIds(Topic("t/a/2"),
                berlinPoint()).toMutableSet()

        // verify berlin
        assertEquals(testIds1.size.toLong(), returnedIds1.size.toLong())
        returnedIds1.removeAll(testIds1)
        assertTrue(returnedIds1.isEmpty())

        assertEquals(testIds2.size.toLong(), returnedIds2.size.toLong())
        returnedIds2.removeAll(testIds2)
        assertTrue(returnedIds2.isEmpty())

        // test hamburg
        returnedIds1 = mapper.getSubscriptionIds(t1, hamburgPoint()).toMutableSet()
        returnedIds2 = mapper.getSubscriptionIds(Topic("t/a/2"), hamburgPoint()).toMutableSet()

        // verify hamburg
        assertEquals(0, returnedIds1.size.toLong())
        assertEquals(0, returnedIds2.size.toLong())
    }

    @Test
    fun testManyGeofencesManyTopics() {
        mapper = TopicAndGeofenceMapper(Configuration())

        // prepare
        val testIds1 = testIds(randomInt(20), randomInt(100))
        val testIds2 = testIds(randomInt(20), randomInt(100))
        logger.info("Generated {} and {} testing ids", testIds1.size, testIds2.size)
        val t1 = Topic("t/1")
        val t2 = Topic("t/+/2")

        // test
        testIds1.forEach { id -> mapper.putSubscriptionId(id, t1, berlinRectangle()) }
        testIds2.forEach { id -> mapper.putSubscriptionId(id, t2, datelineRectangle()) }

        var returnedIds1: MutableSet<ImmutablePair<String, Int>> = mapper.getSubscriptionIds(t1, berlinPoint())
                .toMutableSet()
        var returnedIds2: MutableSet<ImmutablePair<String, Int>> = mapper.getSubscriptionIds(Topic("t/x/2"),
                datelinePoint()).toMutableSet()

        // verify
        assertEquals(testIds1.size.toLong(), returnedIds1.size.toLong())
        returnedIds1.removeAll(testIds1)
        assertTrue(returnedIds1.isEmpty())

        assertEquals(testIds2.size.toLong(), returnedIds2.size.toLong())
        returnedIds2.removeAll(testIds2)
        assertTrue(returnedIds2.isEmpty())

        // test hamburg
        returnedIds1 = mapper.getSubscriptionIds(t1, hamburgPoint()).toMutableSet()
        returnedIds2 = mapper.getSubscriptionIds(Topic("t/a/2"), hamburgPoint()).toMutableSet()

        // verify hamburg
        assertEquals(0, returnedIds1.size.toLong())
        assertEquals(0, returnedIds2.size.toLong())
    }

    @Test
    fun testPutAndThenGet() {
        mapper = TopicAndGeofenceMapper(Configuration(25, 1))

        // prepare
        val testId = ImmutablePair("test-client", 1)
        val t = Topic("data")
        val l = Location(40.007499, 116.320013)
        val f = Geofence.circle(l, 0.01)

        // put
        mapper.putSubscriptionId(testId, t, f)

        // test contained
        assertTrue(mapper.getSubscriptionIds(t, l).contains(testId))
    }

    /*****************************************************************
     * Helpers
     ****************************************************************/

    private fun checkTopicLevels(expected: Array<String>, actual: List<TopicLevel>) {
        val levelSpecifiers = actual.stream().map { tl -> tl.levelSpecifier }.collect(Collectors.toList())
        for (s in expected) {
            assertTrue("$s is missing", levelSpecifiers.remove(s))
        }
        assertTrue("$levelSpecifiers was not expected", levelSpecifiers.isEmpty())
    }

    private fun berlinRectangle(): Geofence {
        return Geofence.polygon(Arrays.asList(Location(53.0, 14.0),
                Location(53.0, 13.0),
                Location(52.0, 13.0),
                Location(52.0, 14.0)))
    }

    private fun datelineRectangle(): Geofence {
        return Geofence.polygon(Arrays.asList(Location(-9.0, 10.0),
                Location(10.0, 10.0),
                Location(10.0, -10.0),
                Location(-9.0, -10.0)))
    }

    private fun berlinPoint(): Location {
        return Location(52.52, 13.405)
    }

    private fun hamburgPoint(): Location {
        return Location(53.511, 9.9937)
    }

    private fun datelinePoint(): Location {
        return Location(-1.2, 1.4)
    }

    private fun testIds(numClient: Int, numIds: Int): Set<ImmutablePair<String, Int>> {
        val seed = System.nanoTime()
        val r = Random(seed)
        logger.info("Random seed for test ids is {}", seed)

        val ids = HashSet<ImmutablePair<String, Int>>()

        for (i in 0 until numClient) {
            val client = randomName(r)
            for (j in 1..numIds) {
                ids.add(ImmutablePair(client, j))
            }
        }

        return ids
    }

}
