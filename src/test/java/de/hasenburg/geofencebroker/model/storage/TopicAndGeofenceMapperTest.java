package de.hasenburg.geofencebroker.model.storage;

import de.hasenburg.geofencebroker.main.Configuration;
import de.hasenburg.geofencebroker.main.Utility;
import de.hasenburg.geofencebroker.model.Topic;
import de.hasenburg.geofencebroker.model.spatial.Geofence;
import de.hasenburg.geofencebroker.model.spatial.Location;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


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

	@Test
	public void testOneGeofenceOneTopic() {
		mapper = new TopicAndGeofenceMapper(new Configuration());

		// prepare
		Set<ImmutablePair<String, Integer>> testIds = testIds(2, 4);
		Topic t = new Topic("test/topic");

		// test berlin
		testIds.forEach(id -> mapper.putSubscriptionId(id, new Topic("test/topic"), berlinRectangle()));
		Set<ImmutablePair<String, Integer>> returnedIds = mapper.getSubscriptionIds(t, berlinPoint());

		// verify berlin
		assertEquals(testIds.size(), returnedIds.size());
		returnedIds.removeAll(testIds);
		assertTrue(returnedIds.isEmpty());

		// test hamburg
		testIds.forEach(id -> mapper.putSubscriptionId(id, new Topic("test/topic"), berlinRectangle()));
		returnedIds = mapper.getSubscriptionIds(t, hamburgPoint());

		// verify hamburg
		assertEquals(0, returnedIds.size());
	}

	private void checkTopicLevels(String[] expected, List<TopicLevel> actual) {
		List<String> levelSpecifiers = actual.stream().map(tl -> tl.getLevelSpecifier()).collect(Collectors.toList());
		for (String s : expected) {
			assertTrue(s + " is missing", levelSpecifiers.remove(s));
		}
		assertTrue(levelSpecifiers.toString() + " was not expected", levelSpecifiers.isEmpty());
	}


	private static Geofence berlinRectangle() {
		return Geofence.polygon(List.of(new Location(53.0, 14.0),
										new Location(53.0, 13.0),
										new Location(52.0, 13.0),
										new Location(52.0, 14.0)));
	}

	private static Location berlinPoint() {
		return new Location(52.52, 13.405);
	}

	private static Location hamburgPoint() {
		return new Location(53.511, 9.9937);
	}


	private static Set<ImmutablePair<String, Integer>> testIds(int numClient, int numIds) {
		long seed = System.nanoTime();
		Random r = new Random(seed);
		logger.info("Random seed for test ids is {}", seed);

		Set<ImmutablePair<String, Integer>> ids = new HashSet<>();

		for (int i = 0; i < numClient; i++) {
			String client = Utility.randomName(r);
			for (int j = 1; j <= numIds; j++) {
				ids.add(new ImmutablePair<>(client, j));
			}
		}

		return ids;
	}

}
