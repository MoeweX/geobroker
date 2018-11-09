package de.hasenburg.geofencebroker.model.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TopicLevelTest {

	private static final Logger logger = LogManager.getLogger();

	TopicLevel anchor;

	@Before
	public void setUpTest() {
		anchor = new TopicLevel("anchor", 1);
	}

	@Test
	public void testCreateTwoDirectChildren() {
		anchor.getOrCreateChildren("child1");
		anchor.getOrCreateChildren("child2");
		assertEquals("child1", anchor.getDirectChild("child1").getLevelSpecifier());
		assertNotNull(anchor.getDirectChild("child1").getRaster());
		assertEquals("child2", anchor.getDirectChild("child2").getLevelSpecifier());
	}

	@Test
	public void testCreateChildrenTree() {
		anchor.getOrCreateChildren("child1", "child2", "child3");
		assertNull(anchor.getDirectChild("child2"));
		assertEquals("child3", anchor.getDirectChild("child1").getDirectChild("child2").getDirectChild("child3")
									 .getLevelSpecifier());
	}

}
