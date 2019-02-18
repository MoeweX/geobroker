package de.hasenburg.geobroker.server.storage;

import de.hasenburg.geobroker.server.main.Configuration;
import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.model.message.Topic;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper;
import de.hasenburg.geobroker.server.storage.TopicLevel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.util.*;
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
		// subscription a/# should produce a match for a, and #
		checkTopicLevels(new String[]{"#", "a", "#"}, mapper.getMatchingTopicLevels(new Topic("a")));

		checkTopicLevels(new String[]{"#"}, mapper.getMatchingTopicLevels(new Topic("b")));
		checkTopicLevels(new String[]{"#", "#"}, mapper.getMatchingTopicLevels(new Topic("a/b/c/d")));
	}

	@Test
	public void testOneGeofenceOneTopic() {
		mapper = new TopicAndGeofenceMapper(new Configuration());

		// prepare
		Set<ImmutablePair<String, Integer>> testIds = testIds(Utility.randomInt(20), Utility.randomInt(100));
		logger.info("Generated {} testing ids", testIds.size());
		Topic t = new Topic("test/topic");

		// test berlin
		testIds.forEach(id -> mapper.putSubscriptionId(id, t, berlinRectangle()));
		Set<ImmutablePair<String, Integer>> returnedIds = mapper.getSubscriptionIds(t, berlinPoint());

		// verify berlin
		assertEquals(testIds.size(), returnedIds.size());
		returnedIds.removeAll(testIds);
		assertTrue(returnedIds.isEmpty());

		// test hamburg
		returnedIds = mapper.getSubscriptionIds(t, hamburgPoint());

		// verify hamburg
		assertEquals(0, returnedIds.size());
	}

	@Test
	public void testOneGeofenceManyTopics() {
		mapper = new TopicAndGeofenceMapper(new Configuration());

		// prepare
		Set<ImmutablePair<String, Integer>> testIds1 = testIds(Utility.randomInt(20), Utility.randomInt(100));
		Set<ImmutablePair<String, Integer>> testIds2 = testIds(Utility.randomInt(20), Utility.randomInt(100));
		logger.info("Generated {} and {} testing ids", testIds1.size(), testIds2.size());
		Topic t1 = new Topic("t/1");
		Topic t2 = new Topic("t/+/2");

		// test berlin
		testIds1.forEach(id -> mapper.putSubscriptionId(id, t1, berlinRectangle()));
		testIds2.forEach(id -> mapper.putSubscriptionId(id, t2, berlinRectangle()));

		Set<ImmutablePair<String, Integer>> returnedIds1 = mapper.getSubscriptionIds(t1, berlinPoint());
		Set<ImmutablePair<String, Integer>> returnedIds2 = mapper.getSubscriptionIds(new Topic("t/a/2"), berlinPoint());

		// verify berlin
		assertEquals(testIds1.size(), returnedIds1.size());
		returnedIds1.removeAll(testIds1);
		assertTrue(returnedIds1.isEmpty());

		assertEquals(testIds2.size(), returnedIds2.size());
		returnedIds2.removeAll(testIds2);
		assertTrue(returnedIds2.isEmpty());

		// test hamburg
		returnedIds1 = mapper.getSubscriptionIds(t1, hamburgPoint());
		returnedIds2 = mapper.getSubscriptionIds(new Topic("t/a/2"), hamburgPoint());

		// verify hamburg
		assertEquals(0, returnedIds1.size());
		assertEquals(0, returnedIds2.size());
	}

	@Test
	public void testManyGeofencesManyTopics() {
		mapper = new TopicAndGeofenceMapper(new Configuration());

		// prepare
		Set<ImmutablePair<String, Integer>> testIds1 = testIds(Utility.randomInt(20), Utility.randomInt(100));
		Set<ImmutablePair<String, Integer>> testIds2 = testIds(Utility.randomInt(20), Utility.randomInt(100));
		logger.info("Generated {} and {} testing ids", testIds1.size(), testIds2.size());
		Topic t1 = new Topic("t/1");
		Topic t2 = new Topic("t/+/2");

		// test
		testIds1.forEach(id -> mapper.putSubscriptionId(id, t1, berlinRectangle()));
		testIds2.forEach(id -> mapper.putSubscriptionId(id, t2, datelineRectangle()));

		Set<ImmutablePair<String, Integer>> returnedIds1 = mapper.getSubscriptionIds(t1, berlinPoint());
		Set<ImmutablePair<String, Integer>> returnedIds2 =
				mapper.getSubscriptionIds(new Topic("t/x/2"), datelinePoint());

		// verify
		assertEquals(testIds1.size(), returnedIds1.size());
		returnedIds1.removeAll(testIds1);
		assertTrue(returnedIds1.isEmpty());

		assertEquals(testIds2.size(), returnedIds2.size());
		returnedIds2.removeAll(testIds2);
		assertTrue(returnedIds2.isEmpty());

		// test hamburg
		returnedIds1 = mapper.getSubscriptionIds(t1, hamburgPoint());
		returnedIds2 = mapper.getSubscriptionIds(new Topic("t/a/2"), hamburgPoint());

		// verify hamburg
		assertEquals(0, returnedIds1.size());
		assertEquals(0, returnedIds2.size());
	}

	@Test
	public void testPutAndThenGet() {
		mapper = new TopicAndGeofenceMapper(new Configuration(25, 1));

		// prepare
		ImmutablePair<String, Integer> testId = new ImmutablePair<>("test-client", 1);
		Topic t = new Topic("data");
		Location l = new Location(40.007499, 116.320013);
		Geofence f = Geofence.circle(l, 0.01);

		// put
		mapper.putSubscriptionId(testId, t, f);

		// test contained
		assertTrue(mapper.getSubscriptionIds(t, l).contains(testId));
	}


	private void checkTopicLevels(String[] expected, List<TopicLevel> actual) {
		List<String> levelSpecifiers = actual.stream().map(tl -> tl.getLevelSpecifier()).collect(Collectors.toList());
		for (String s : expected) {
			assertTrue(s + " is missing", levelSpecifiers.remove(s));
		}
		assertTrue(levelSpecifiers.toString() + " was not expected", levelSpecifiers.isEmpty());
	}


	private static Geofence berlinRectangle() {
		return Geofence.polygon(Arrays.asList(new Location(53.0, 14.0),
											  new Location(53.0, 13.0),
											  new Location(52.0, 13.0),
											  new Location(52.0, 14.0)));
	}

	private static Geofence datelineRectangle() {
		return Geofence.polygon(Arrays.asList(new Location(-9, 10),
											  new Location(10, 10),
											  new Location(10, -10),
											  new Location(-9, -10)));
	}

	private static Location berlinPoint() {
		return new Location(52.52, 13.405);
	}

	private static Location hamburgPoint() {
		return new Location(53.511, 9.9937);
	}

	private static Location datelinePoint() {
		return new Location(-1.2, 1.4);
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
