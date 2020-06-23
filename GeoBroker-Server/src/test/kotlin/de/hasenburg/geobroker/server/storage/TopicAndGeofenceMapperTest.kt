package de.hasenburg.geobroker.server.storage

import de.hasenburg.geobroker.commons.model.message.Topic
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.randomInt
import de.hasenburg.geobroker.commons.randomName
import de.hasenburg.geobroker.server.main.Configuration
import de.hasenburg.geobroker.server.storage.client.ClientDirectory
import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.logging.log4j.LogManager
import org.junit.Assert.*
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
        var returnedIds = mapper.getPotentialSubscriptionIds(t, berlinPoint()).toMutableSet()

        // verify berlin
        assertEquals(testIds.size.toLong(), returnedIds.size.toLong())
        returnedIds.removeAll(testIds)
        assertTrue(returnedIds.isEmpty())

        // test hamburg
        returnedIds = mapper.getPotentialSubscriptionIds(t, hamburgPoint()).toMutableSet()

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

        var returnedIds1: MutableSet<ImmutablePair<String, Int>> =
                mapper.getPotentialSubscriptionIds(t1, berlinPoint()).toMutableSet()
        var returnedIds2: MutableSet<ImmutablePair<String, Int>> =
                mapper.getPotentialSubscriptionIds(Topic("t/a/2"), berlinPoint()).toMutableSet()

        // verify berlin
        assertEquals(testIds1.size.toLong(), returnedIds1.size.toLong())
        returnedIds1.removeAll(testIds1)
        assertTrue(returnedIds1.isEmpty())

        assertEquals(testIds2.size.toLong(), returnedIds2.size.toLong())
        returnedIds2.removeAll(testIds2)
        assertTrue(returnedIds2.isEmpty())

        // test hamburg
        returnedIds1 = mapper.getPotentialSubscriptionIds(t1, hamburgPoint()).toMutableSet()
        returnedIds2 = mapper.getPotentialSubscriptionIds(Topic("t/a/2"), hamburgPoint()).toMutableSet()

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

        var returnedIds1: MutableSet<ImmutablePair<String, Int>> =
                mapper.getPotentialSubscriptionIds(t1, berlinPoint()).toMutableSet()
        var returnedIds2: MutableSet<ImmutablePair<String, Int>> =
                mapper.getPotentialSubscriptionIds(Topic("t/x/2"), datelinePoint()).toMutableSet()

        // verify
        assertEquals(testIds1.size.toLong(), returnedIds1.size.toLong())
        returnedIds1.removeAll(testIds1)
        assertTrue(returnedIds1.isEmpty())

        assertEquals(testIds2.size.toLong(), returnedIds2.size.toLong())
        returnedIds2.removeAll(testIds2)
        assertTrue(returnedIds2.isEmpty())

        // test hamburg
        returnedIds1 = mapper.getPotentialSubscriptionIds(t1, hamburgPoint()).toMutableSet()
        returnedIds2 = mapper.getPotentialSubscriptionIds(Topic("t/a/2"), hamburgPoint()).toMutableSet()

        // verify hamburg
        assertEquals(0, returnedIds1.size.toLong())
        assertEquals(0, returnedIds2.size.toLong())
    }

    @Test
    fun testPutAndThenGet() {
        mapper = TopicAndGeofenceMapper(Configuration(granularity = 25, messageProcessors = 1))

        // prepare
        val testId = ImmutablePair("test-client", 1)
        val t = Topic("data")
        val l = Location(40.007499, 116.320013)
        val f = Geofence.circle(l, 0.01)

        // put
        mapper.putSubscriptionId(testId, t, f)

        // test contained
        assertTrue(mapper.getPotentialSubscriptionIds(t, l).contains(testId))
    }

    @Test
    fun testWorld() {
        mapper = TopicAndGeofenceMapper(Configuration(granularity = 1, messageProcessors = 1))

        // prepare
        val top = Topic("a")
        val l1 = Location(40.1, 116.3)
        val g1 = Geofence.circle(l1, 1.5)
        val t1 = ImmutablePair("t1", 1)
        val l2 = Location(73.1, 105.3)
        val g2 = Geofence.circle(l2, 1.5)
        val t2 = ImmutablePair("t2", 1)
        val w = Geofence.world()
        val t3 = ImmutablePair("t3", 1)

        // put
        mapper.putSubscriptionId(t1, top, g1)
        mapper.putSubscriptionId(t2, top, g2)
        mapper.putSubscriptionId(t3, top, w)

        // tests
        assertTrue(mapper.getPotentialSubscriptionIds(top, l1).contains(t1))
        assertFalse(mapper.getPotentialSubscriptionIds(top, l1).contains(t2))
        assertTrue(mapper.getPotentialSubscriptionIds(top, l1).contains(t3))

        assertFalse(mapper.getPotentialSubscriptionIds(top, l2).contains(t1))
        assertTrue(mapper.getPotentialSubscriptionIds(top, l2).contains(t2))
        assertTrue(mapper.getPotentialSubscriptionIds(top, l2).contains(t3))
    }

    @Test
    fun randomTest() {
        val td = ClientDirectory()

        mapper = TopicAndGeofenceMapper(Configuration(granularity = 1, messageProcessors = 1))

        // tested area
        val testedArea = Geofence.circle(Location(10.0, 10.0), 2.0)

        // prepare
        val t1 = Topic("sensor/temperature")
        val t1List = mutableListOf<Geofence>()
        val t2 = Topic("sensor/car")
        val t2List = mutableListOf<Geofence>()

        // put t1 topics
        for (i in 0..200) {
            val clientIdentifier = "t1-client-" + i
            val g = Geofence.circle(Location.randomInGeofence(testedArea)!!, 0.5)
            td.addClient(clientIdentifier, null)
            val id = td.updateSubscription(clientIdentifier, t1, g)!!
            mapper.putSubscriptionId(id, t1, g)
            t1List.add(g)
        }

        // put t2 topics
        for (i in 0..200) {
            val clientIdentifier = "t2-client-" + i
            val g = Geofence.circle(Location.randomInGeofence(testedArea)!!, 0.5)
            td.addClient(clientIdentifier, null)
            val id = td.updateSubscription(clientIdentifier, t2, g)!!
            mapper.putSubscriptionId(id, t2, g)
            t2List.add(g)
        }

        // now test randomly locations for t1
        for (i in 0..10) {
            val matchingIndices = mutableListOf<Int>()
            val publisherLocation = Location.randomInGeofence(testedArea)!!
            for ((j, geofence) in t1List.withIndex()) {
                if (geofence.contains(publisherLocation)) {
                    matchingIndices.add(j)
                }
            }

            val mapperResultIndices = mapper.getSubscriptionIds(t1, publisherLocation, td).stream().map { id -> id.right }
                    .sorted(naturalOrder()).collect(Collectors.toList())
            assertEquals(matchingIndices.size, mapperResultIndices.size)
        }

        // now test randomly locations for t2
        for (i in 0..10) {
            val matchingIndices = mutableListOf<Int>()
            val publisherLocation = Location.randomInGeofence(testedArea)!!
            for ((j, geofence) in t2List.withIndex()) {
                if (geofence.contains(publisherLocation)) {
                    matchingIndices.add(j)
                }
            }

            val mapperResultIndices = mapper.getSubscriptionIds(t2, publisherLocation, td).stream().map { id -> id.right }
                    .sorted(naturalOrder()).collect(Collectors.toList())
            assertEquals(matchingIndices.size, mapperResultIndices.size)
        }

    }

    @Test
    fun specificTest() {
        val g = Geofence.fromWkt("BUFFER (POINT (8.079053798283907 10.017496679172208), 0.5)")

        val td = ClientDirectory()
        td.addClient("c", null)
        val id = td.updateSubscription("c", Topic("t"), g)!!

        mapper = TopicAndGeofenceMapper(Configuration(granularity = 25, messageProcessors = 1))
        val pl = Location(9.55051768336062, 8.299518376483809)
        mapper.putSubscriptionId(id, Topic("t"), g)

        assertFalse(g.contains(pl))
        assertTrue(mapper.getSubscriptionIds(Topic("t"), pl, td).size == 0)
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
