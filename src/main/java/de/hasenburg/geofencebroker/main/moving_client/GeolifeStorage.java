package de.hasenburg.geofencebroker.main.moving_client;

import de.hasenburg.geofencebroker.main.Configuration;
import de.hasenburg.geofencebroker.main.Utility;
import de.hasenburg.geofencebroker.model.Topic;
import de.hasenburg.geofencebroker.model.spatial.Geofence;
import de.hasenburg.geofencebroker.model.spatial.Location;
import de.hasenburg.geofencebroker.model.storage.TopicAndGeofenceMapper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class stresses only the storage in a similar fashion and the {@link ClientManager} and {@link GeolifeBroker}
 * stress the whole broker.
 */
@SuppressWarnings("FieldCanBeLocal")
public class GeolifeStorage {

	/*****************************************************************
	 * CONFIGURATION
	 ****************************************************************/

	private final int GRANULARITY = 100;
	private final int MESSAGE_PROCESSORS = 2;

	private final int GEOLIFE_START_INDEX = 1;
	private final int GEOLIFE_STOP_INDEX = 5;

	private final double GEOFENCE_SIZE = 0.01; // in degree

	private final int UPDATE_OPS = 1; // update operations before doing publish operations
	private final int PUBLISH_OPS = 4; // publish operations before resetting counters

	private final int EXPERIMENT_TIME = 1; // min

	/*****************************************************************
	 * END CONFIGURATION
	 ****************************************************************/

	private static final Logger logger = LogManager.getLogger();

	private final TopicAndGeofenceMapper storage;
	private final ExecutorService executorService;

	public GeolifeStorage() {
		logger.info("Creating Storage");
		Configuration c = new Configuration(GRANULARITY, MESSAGE_PROCESSORS);
		this.storage = new TopicAndGeofenceMapper(c);

		logger.info("Creating ExecutorService");
		this.executorService = Executors.newFixedThreadPool(GEOLIFE_STOP_INDEX - GEOLIFE_START_INDEX + 1);

		logger.info("Calculating Routes");
		GeolifeDatasetHelper gdh = new GeolifeDatasetHelper();
		gdh.prepare();
		Map<Integer, Route> routes = gdh.downloadRequiredFiles(GEOLIFE_START_INDEX, GEOLIFE_STOP_INDEX);

		logger.info("Creating and starting clients");
		for (Integer index : routes.keySet()) {
			Route r = routes.get(index);
			executorService.submit(new Client(r, "client-" + index, UPDATE_OPS, PUBLISH_OPS));
		}

		logger.info("Sleeping for {} min", EXPERIMENT_TIME);
		Utility.sleepNoLog((long) (EXPERIMENT_TIME * 60 * 1000), 0);

		logger.info("Shutting down executor");
		executorService.shutdown();
	}

	private class Client implements Runnable {

		private final Route route;
		private final String identity;
		private final ImmutablePair<String, Integer> subscriptionId;
		private final int updateOps;
		private final int publishOps;

		public Client(Route route, String identity, int updateOps, int publishOps) {
			this.route = route;
			this.identity = identity;
			this.subscriptionId = new ImmutablePair<>(identity, 1);
			this.updateOps = updateOps;
			this.publishOps = publishOps;
		}

		@Override
		public void run() {
			Thread.currentThread().setName(identity);
			logger.info("{} started to run operations against storage", identity);

			Topic topic = new Topic("data");
			int numberOfOperations = 0;
			Long timestamp = System.nanoTime();
			Geofence lastFence = null;
			int numUpdateOps = 0;
			int numPublishOps = 0;

			while (!Thread.currentThread().isInterrupted()) {
				if (numberOfOperations >= 100000) {
					long timespan = (System.nanoTime() - timestamp) / 1000 / 1000; // milliseconds
					double timespanSec = timespan / 1000.0;
					int opsPerSecond = (int) (numberOfOperations / timespanSec);
					logger.info("Did {} operations in {} seconds ({} ops/second)", numberOfOperations, timespanSec, opsPerSecond);
					numberOfOperations = 0;
					timestamp = System.nanoTime();
				}
				for (ImmutablePair<Location, Integer> visitedLocation : route.getVisitedLocations()) {
					numberOfOperations += 1;
					Geofence fence = Geofence.circle(visitedLocation.left, GEOFENCE_SIZE);

					if (numUpdateOps < updateOps) {
						numUpdateOps += 1;
						// update subscription
						storage.removeSubscriptionId(subscriptionId, topic, lastFence);
						storage.putSubscriptionId(subscriptionId, topic, fence);
						lastFence = fence;
					} else if (numPublishOps < publishOps) {
						// find publish targets
						storage.getSubscriptionIds(topic, visitedLocation.left); // I am currently at visitedLocation
						numPublishOps += 1;
					} else {
						numUpdateOps = 0;
						numPublishOps = 0;
					}

				}
			}
		}

	}

	public static void main (String[] args) {
	    GeolifeStorage gs = new GeolifeStorage();
	}

}
