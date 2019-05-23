package de.hasenburg.geobroker.server.storage

import org.apache.logging.log4j.LogManager
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

private val logger = LogManager.getLogger()

class TopicLevelTest {

    private lateinit var anchor: TopicLevel

    @Before
    fun setUpTest() {
        anchor = TopicLevel("anchor", 1)
    }

    @Test
    fun testCreateTwoDirectChildren() {
        anchor.getOrCreateChildren("child1")
        anchor.getOrCreateChildren("child2")
        assertEquals("child1", anchor.getDirectChild("child1")?.levelSpecifier)
        assertNotNull(anchor.getDirectChild("child1")?.raster)
        assertEquals("child2", anchor.getDirectChild("child2")?.levelSpecifier)
    }

    @Test
    fun testCreateChildrenTree() {
        anchor.getOrCreateChildren("child1", "child2", "child3")
        assertNull(anchor.getDirectChild("child2"))
        assertEquals("child3",
                anchor.getDirectChild("child1")?.getDirectChild("child2")?.getDirectChild("child3")?.levelSpecifier)
    }

}
