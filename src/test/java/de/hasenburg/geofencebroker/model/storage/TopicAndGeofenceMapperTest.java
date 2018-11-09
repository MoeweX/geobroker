package de.hasenburg.geofencebroker.model.storage;

import de.hasenburg.geofencebroker.main.Configuration;
import de.hasenburg.geofencebroker.model.Topic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;


public class TopicAndGeofenceMapperTest {

	private static final Logger logger = LogManager.getLogger();

	TopicAndGeofenceMapper mapper;

	@Test
	public void testGetMatchingTopicLevels_NoWildcards() {
		mapper = new TopicAndGeofenceMapper(new Configuration());
		mapper.getAnchor().getOrCreateChildren("a", "b", "c");
		mapper.getAnchor().getOrCreateChildren("a", "b", "d");
		checkTopicLevels(new String[]{"c"}, mapper.getMatchingTopicLevels(new Topic("a/b/c")));
		checkTopicLevels(new String[]{"b"}, mapper.getMatchingTopicLevels(new Topic("a/b")));
		checkTopicLevels(new String[]{}, mapper.getMatchingTopicLevels(new Topic("b")));
	}

	@Test
	public void testGetMatchingTopicLevels_SingleLevelWildcards() {
		mapper = new TopicAndGeofenceMapper(new Configuration());
		mapper.getAnchor().getOrCreateChildren("a", "b", "c");
		mapper.getAnchor().getOrCreateChildren("a", "b", "+");
		mapper.getAnchor().getOrCreateChildren("a", "+", "c");
		checkTopicLevels(new String[]{"c", "+", "c"}, mapper.getMatchingTopicLevels(new Topic("a/b/c")));
		checkTopicLevels(new String[]{"+"}, mapper.getMatchingTopicLevels(new Topic("a/b/x")));
		checkTopicLevels(new String[]{"+"}, mapper.getMatchingTopicLevels(new Topic("a/x")));
	}

	@Test
	public void testGetMatchingTopicLevels_MultiLevelWildcards() {
		mapper = new TopicAndGeofenceMapper(new Configuration());
		mapper.getAnchor().getOrCreateChildren("a", "b", "c");
		mapper.getAnchor().getOrCreateChildren("a", "b", "+");
		mapper.getAnchor().getOrCreateChildren("a", "+", "c");
		mapper.getAnchor().getOrCreateChildren("a", "#");
		mapper.getAnchor().getOrCreateChildren("#");
		checkTopicLevels(new String[]{"c", "+", "c", "#", "#"}, mapper.getMatchingTopicLevels(new Topic("a/b/c")));
		checkTopicLevels(new String[]{"+", "#", "#"}, mapper.getMatchingTopicLevels(new Topic("a/b/x")));
		checkTopicLevels(new String[]{"+", "#", "#"}, mapper.getMatchingTopicLevels(new Topic("a/x")));
		checkTopicLevels(new String[]{"#", "a"}, mapper.getMatchingTopicLevels(new Topic("a")));
		checkTopicLevels(new String[]{"#"}, mapper.getMatchingTopicLevels(new Topic("b")));
		checkTopicLevels(new String[]{"#", "#"}, mapper.getMatchingTopicLevels(new Topic("a/b/c/d")));
	}

	private void checkTopicLevels(String[] expected, List<TopicLevel> actual) {
		List<String> levelSpecifiers = actual.stream().map(tl -> tl.getLevelSpecifier()).collect(Collectors.toList());
		for (String s : expected) {
			assertTrue(s + " is missing", levelSpecifiers.remove(s));
		}
		assertTrue(levelSpecifiers.toString() + " was not expected", levelSpecifiers.isEmpty());
	}


}
