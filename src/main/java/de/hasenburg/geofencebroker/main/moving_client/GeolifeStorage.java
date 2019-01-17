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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * This class stresses only the storage in a similar fashion and the {@link ClientManager} and {@link GeolifeBroker}
 * stress the whole broker.
 */
@SuppressWarnings("FieldCanBeLocal")
public class GeolifeStorage {

	/*****************************************************************
	 * CONFIGURATION
	 ****************************************************************/

	private static final int GRANULARITY = 100;
	private static final Integer[] MESSAGE_PROCESSORS = {1, 2, 4, 8, 16, 32};

	private static final int GEOLIFE_START_INDEX = 1;
	private static final Integer[] GEOLIFE_STOP_INDEX = {1, 10, 100, 1000};

	private static final double GEOFENCE_SIZE = 0.01; // in degree

	// update operations before doing publish operations
	// publish operations before resetting counters
	private static final String[] UPDATE_PUBLISH_OPS = {"1,99", "1,1", "99,1"};
			// update operations before doing publish operations

	private static final int EXPERIMENT_TIME = 60; // sec

	/*****************************************************************
	 * END CONFIGURATION
	 ****************************************************************/

	private static final Logger logger = LogManager.getLogger();

	private final TopicAndGeofenceMapper storage;
	private final ExecutorService executorService;

	public GeolifeStorage(int GRANULARITY, int MESSAGE_PROCESSORS, int GEOLIFE_START_INDEX, int GEOLIFE_STOP_INDEX,
						  double GEOFENCE_SIZE, int UPDATE_OPS, int PUBLISH_OPS, int EXPERIMENT_TIME) {
		logger.debug("Creating Storage");
		Configuration c = new Configuration(GRANULARITY, MESSAGE_PROCESSORS);
		this.storage = new TopicAndGeofenceMapper(c);

		logger.debug("Creating ExecutorService");
		this.executorService = Executors.newFixedThreadPool(GEOLIFE_STOP_INDEX - GEOLIFE_START_INDEX + 1);

		logger.debug("Calculating Routes");
		GeolifeDatasetHelper gdh = new GeolifeDatasetHelper();
		gdh.prepare();
		Map<Integer, Route> routes = gdh.downloadRequiredFiles(GEOLIFE_START_INDEX, GEOLIFE_STOP_INDEX);

		logger.debug("Creating and starting clients");
		List<Future<Integer>> throughputF = new ArrayList<>();
		for (Integer index : routes.keySet()) {
			Route r = routes.get(index);
			throughputF.add(executorService.submit(new Client(r,
															  "client-" + index,
															  UPDATE_OPS,
															  PUBLISH_OPS,
															  GEOFENCE_SIZE)));
		}

		logger.debug("Sleeping for {} sec", EXPERIMENT_TIME);
		Utility.sleepNoLog((long) (EXPERIMENT_TIME  * 1000), 0);

		logger.debug("Shutting down executor");
		executorService.shutdownNow();
		List<Integer> throughputs = new ArrayList<>();
		for (Future<Integer> future : throughputF) {
			try {
				throughputs.add(future.get());
			} catch (InterruptedException e) {
				logger.error("Program terminated while waiting on client result", e);
			} catch (ExecutionException e) {
				logger.error("Client could not finish due to exception", e);

			}
		}

		logger.info("GEOLIFE_STOP_INDEX={},MESSAGE_PROCESSORS={},UPDATE_OPS={},PUBLISH_OPS={},Throughput (ops/s)={}",
					GEOLIFE_STOP_INDEX,
					MESSAGE_PROCESSORS,
					UPDATE_OPS,
					PUBLISH_OPS,
					throughputs.stream().mapToInt(Integer::intValue).average().getAsDouble());
	}

	private class Client implements Callable<Integer> {

		private final Route route;
		private final String identity;
		private final ImmutablePair<String, Integer> subscriptionId;
		private final int updateOps;
		private final int publishOps;

		private final double GEOFENCE_SIZE;

		public Client(Route route, String identity, int updateOps, int publishOps, double GEOFENCE_SIZE) {
			this.route = route;
			this.identity = identity;
			this.subscriptionId = new ImmutablePair<>(identity, 1);
			this.updateOps = updateOps;
			this.publishOps = publishOps;
			this.GEOFENCE_SIZE = GEOFENCE_SIZE;
		}

		@Override
		public Integer call() {
			Thread.currentThread().setName(identity);
			logger.debug("{} started to run operations against storage", identity);

			Topic topic = new Topic("data");
			int numberOfOperations = 0;
			Long timestamp = System.nanoTime();
			Geofence lastFence = null;
			int numUpdateOps = 0;
			int numPublishOps = 0;

			while (!Thread.currentThread().isInterrupted()) {
				for (ImmutablePair<Location, Integer> visitedLocation : route.getVisitedLocations()) {
					numberOfOperations += 1;
					Geofence fence = Geofence.circle(visitedLocation.left, GEOFENCE_SIZE);

					if (numUpdateOps < updateOps) {
						numUpdateOps += 1;
						// update subscription
						if (lastFence != null) {
							storage.removeSubscriptionId(subscriptionId, topic, lastFence);
						}
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
			long timespan = (System.nanoTime() - timestamp) / 1000 / 1000; // milliseconds
			double timespanSec = timespan / 1000.0;
			int opsPerSecond = (int) (numberOfOperations / timespanSec);
			logger.debug("{} finished to run operations against storage, ran {} operations per second",
						 identity,
						 opsPerSecond);
			return opsPerSecond;
		}
	}

	public static void main(String[] args) {

		logger.info("Doing the GeolifeStorage Experiment");
		logger.info("GRANULARITY = {}", GRANULARITY);
		logger.info("GEOFENCE_SIZE = {}", GEOFENCE_SIZE);
		logger.info("EXPERIMENT_TIME = {}", EXPERIMENT_TIME);
		logger.info("GEOLIFE_START_INDEX = {}", GEOLIFE_START_INDEX);
		logger.info("GEOLIFE_STOP_INDEX = {}", Arrays.asList(GEOLIFE_STOP_INDEX));
		logger.info("MESSAGE_PROCESSORS = {}", Arrays.asList(MESSAGE_PROCESSORS));
		logger.info("UPDATE_PUBLISH_OPS = {}", Arrays.asList(UPDATE_PUBLISH_OPS));

		for (Integer geolifeStopIndex : GEOLIFE_STOP_INDEX) {
			for (Integer messageProcessors : MESSAGE_PROCESSORS) {
				for (String updatePublishOps : UPDATE_PUBLISH_OPS) {
					int UPDATE_OPS = Integer.parseInt(updatePublishOps.split(",")[0]);
					int PUBLISH_OPS = Integer.parseInt(updatePublishOps.split(",")[1]);

					GeolifeStorage gs = new GeolifeStorage(GRANULARITY,
														   messageProcessors,
														   GEOLIFE_START_INDEX,
														   geolifeStopIndex,
														   GEOFENCE_SIZE,
														   UPDATE_OPS,
														   PUBLISH_OPS,
														   EXPERIMENT_TIME);
				}
			}
		}

	}



}
