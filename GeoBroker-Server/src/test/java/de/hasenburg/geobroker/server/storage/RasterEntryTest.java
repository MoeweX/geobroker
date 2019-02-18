package de.hasenburg.geobroker.server.storage;

import de.hasenburg.geobroker.commons.Utility;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import de.hasenburg.geobroker.server.storage.RasterEntry;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class RasterEntryTest {

	private static final Logger logger = LogManager.getLogger();

	static {
		Utility.setLogLevel(logger, Level.INFO);
	}

	private ExecutorService executorService;
	private final int THREADS = 10;
	private final int OPERATIONS_PER_CLIENT = 10000;

	@Before
	public void setUp() {
		executorService = Executors.newFixedThreadPool(THREADS);
	}

	@After
	public void tearDown() {
		executorService.shutdownNow();
		assertTrue(executorService.isShutdown());
	}

	/*****************************************************************
	 * Functionality
	 ****************************************************************/

	@Test
	public void testSubscribeUnsubscribe() {
		RasterEntry rasterEntry = new RasterEntry(Location.random(), 1.0);
		ImmutablePair<String, Integer> subscriptionId = new ImmutablePair<>("client", 1);
		rasterEntry.putSubscriptionId(subscriptionId);
		assertEquals(1, rasterEntry.getNumSubscriptionIds().intValue());
		assertEquals(1, rasterEntry.getAllSubscriptionIds().size());
		rasterEntry.removeSubscriptionId(subscriptionId);
		assertEquals(0, rasterEntry.getNumSubscriptionIds().intValue());
		assertNull(rasterEntry.getAllSubscriptionIds().get(subscriptionId));
		assertEquals(1,
					 rasterEntry
							 .getAllSubscriptionIds()
							 .size()); // fine, as the HashSet is empty, even though the key persists
		assertEquals(0,
					 rasterEntry.getSubscriptionIdsForClientIdentifier(subscriptionId.left).size()); // see, it is empty
	}

	/*****************************************************************
	 * Immutability
	 ****************************************************************/

	@Test(expected = UnsupportedOperationException.class)
	public void testGetAllImmutability() throws InterruptedException {
		RasterEntry rasterEntry = new RasterEntry(Location.random(), 1);

		for (int i = 0; i < THREADS; i++) {
			String clientIdentifier = System.nanoTime() + "";
			// every client has its own client identifier and ids are not synchronized
			executorService.submit(new FakeClientCallable(clientIdentifier,
														  OPERATIONS_PER_CLIENT,
														  rasterEntry,
														  new AtomicInteger(0)));
		}

		executorService.shutdown();
		executorService.awaitTermination(10, TimeUnit.SECONDS);

		// small speed test
		for (int i = 0; i < 100 * OPERATIONS_PER_CLIENT; i++) {
			int size = rasterEntry.getAllSubscriptionIds().size();
		}

		rasterEntry
				.getAllSubscriptionIds()
				.put("fail", Stream.of(ImmutablePair.of("fail", 1)).collect(Collectors.toSet()));
	}

	/*****************************************************************
	 * RasterEntryBox
	 ****************************************************************/

	@Test
	public void testRasterEntryBox() {
		RasterEntry entry = new RasterEntry(new Location(1.5, 1.2), 1);
		Geofence expectedBox = Geofence.polygon(Arrays.asList(new Location(1.5, 1.2),
															  new Location(2.5, 1.2),
															  new Location(2.5, 2.2),
															  new Location(1.5, 2.2)));
		assertEquals(expectedBox, entry.getRasterEntryBox());
	}

	/*****************************************************************
	 * Threading
	 ****************************************************************/

	@Test
	public void testSingleThreaded() throws InterruptedException, ExecutionException, TimeoutException {
		RasterEntry rasterEntry = new RasterEntry(Location.random(), 1);
		String clientIdentifier = "U3";
		Future<Set<ImmutablePair<String, Integer>>> f = executorService.submit(new FakeClientCallable(clientIdentifier,
																									  OPERATIONS_PER_CLIENT,
																									  rasterEntry,
																									  new AtomicInteger(
																											  0)));
		Set<ImmutablePair<String, Integer>> resultList = f.get(3, TimeUnit.SECONDS);

		// test size
		assertEquals(resultList.size(), rasterEntry.getNumSubscriptionIds().intValue());
		logger.info("Raster entry stores {} subscriptionIds", rasterEntry.getNumSubscriptionIds());

		// compare content
		resultList.removeAll(rasterEntry.getSubscriptionIdsForClientIdentifier(clientIdentifier));
		assertEquals(0, resultList.size());
		logger.info("SubscriptionsIds of client {} match", clientIdentifier);
	}

	/**
	 * Clients are ALLOWED to add subscriptionIds concurrently for different clientIds without inconsistencies.
	 */
	@Test
	public void testMultiThreadedDifferentClientIds()
			throws InterruptedException, ExecutionException, TimeoutException {
		RasterEntry rasterEntry = new RasterEntry(Location.random(), 1);
		HashMap<String, Future<Set<ImmutablePair<String, Integer>>>> futures = new HashMap<>();

		for (int i = 0; i < THREADS; i++) {
			String clientIdentifier = System.nanoTime() + "";
			// every client has its own client identifier and ids are not synchronized
			futures.put(clientIdentifier,
						executorService.submit(new FakeClientCallable(clientIdentifier,
																	  OPERATIONS_PER_CLIENT,
																	  rasterEntry,
																	  new AtomicInteger(0))));
		}

		executorService.shutdown();
		executorService.awaitTermination(10, TimeUnit.SECONDS);

		int sum = 0;
		for (Map.Entry<String, Future<Set<ImmutablePair<String, Integer>>>> item : futures.entrySet()) {
			String clientIdentifier = item.getKey();
			Future<Set<ImmutablePair<String, Integer>>> future = item.getValue();

			// add to sum
			Set<ImmutablePair<String, Integer>> idsFromThread = future.get(1, TimeUnit.SECONDS);
			sum += idsFromThread.size();

			// check content
			Set<ImmutablePair<String, Integer>> idsFromRaster =
					rasterEntry.getSubscriptionIdsForClientIdentifier(clientIdentifier);
			assertEquals(idsFromThread, idsFromRaster);
			logger.info("SubscriptionsIds of client {} match", clientIdentifier);
		}

		// check size
		assertEquals(sum, rasterEntry.getNumSubscriptionIds().intValue());
		logger.info("Raster entry stored {} subscriptionIds", rasterEntry.getNumSubscriptionIds());

	}

	/**
	 * Clients are ALLOWED to add subscriptionIds concurrently for similar clientIds if each subscriptionId is unique
	 * without inconsistencies.
	 */
	@Test
	public void testMultiThreadedSameClientIdSynchronized()
			throws InterruptedException, ExecutionException, TimeoutException {
		RasterEntry rasterEntry = new RasterEntry(Location.random(), 1);
		List<Future<Set<ImmutablePair<String, Integer>>>> futures = new ArrayList<>();
		AtomicInteger atomicInteger = new AtomicInteger();
		String clientIdentifier = "U3";

		for (int i = 0; i < THREADS; i++) {
			// every client has its own client identifier and ids are synchronized via an atomic integer
			futures.add(executorService.submit(new FakeClientCallable(clientIdentifier,
																	  OPERATIONS_PER_CLIENT,
																	  rasterEntry,
																	  atomicInteger)));
		}

		executorService.shutdown();
		executorService.awaitTermination(10, TimeUnit.SECONDS);

		Set<ImmutablePair<String, Integer>> idsFromRaster =
				rasterEntry.getSubscriptionIdsForClientIdentifier(clientIdentifier);
		Set<ImmutablePair<String, Integer>> idsFromThreads = new HashSet<>();

		int sum = 0;
		for (Future<Set<ImmutablePair<String, Integer>>> future : futures) {
			// add to sum
			Set<ImmutablePair<String, Integer>> idsFromThread = future.get(1, TimeUnit.SECONDS);
			sum += idsFromThread.size();

			// add to idsFromThreads
			idsFromThreads.addAll(idsFromThread);
		}

		// check size
		assertEquals(sum, rasterEntry.getNumSubscriptionIds().intValue());
		logger.info("Raster entry stored {} subscriptionIds", rasterEntry.getNumSubscriptionIds());

		// check if all ids in raster have been in threads lists
		assertEquals(idsFromThreads, idsFromRaster);
		logger.info("SubscriptionsIds of client {} match", clientIdentifier);
	}

	/**
	 * Clients are NOT ALLOWED to add subscriptionIds concurrently for similar clientIds if subscriptionIds are not
	 * unique -> leads to INCONSISTENCIES
	 */
	@Test
	public void testMultiThreadedSameClientIdNotSynchronized()
			throws InterruptedException, ExecutionException, TimeoutException {
		RasterEntry rasterEntry = new RasterEntry(Location.random(), 1);
		List<Future<Set<ImmutablePair<String, Integer>>>> futures = new ArrayList<>();
		String clientIdentifier = "U3";

		for (int i = 0; i < THREADS; i++) {
			// every client has its own client identifier and ids are synchronized via an atomic integer
			futures.add(executorService.submit(new FakeClientCallable(clientIdentifier,
																	  OPERATIONS_PER_CLIENT,
																	  rasterEntry,
																	  new AtomicInteger())));
		}

		executorService.shutdown();
		executorService.awaitTermination(10, TimeUnit.SECONDS);

		Set<ImmutablePair<String, Integer>> idsFromRaster =
				rasterEntry.getSubscriptionIdsForClientIdentifier(clientIdentifier);
		Set<ImmutablePair<String, Integer>> idsFromThreads = new HashSet<>();

		int sum = 0;
		for (Future<Set<ImmutablePair<String, Integer>>> future : futures) {
			// add to sum
			Set<ImmutablePair<String, Integer>> idsFromThread = future.get(1, TimeUnit.SECONDS);
			sum += idsFromThread.size();

			// add to idsFromThreads
			idsFromThreads.addAll(idsFromThread);
		}

		// check size
		assertNotEquals(sum, rasterEntry.getNumSubscriptionIds().intValue());
		logger.info("Raster entry stored {} subscriptionIds, threads stored {}",
					rasterEntry.getNumSubscriptionIds(),
					sum);

		// check if all ids in raster have been in threads lists
		assertNotEquals(idsFromThreads, idsFromRaster);
		logger.info("Raster entry has not subscriptionIds that have not been added for client {}", clientIdentifier);
	}

	private class FakeClientCallable implements Callable<Set<ImmutablePair<String, Integer>>> {

		private final String clientIdentifier;
		private final int numberOfOperations;
		private final RasterEntry rasterEntry;
		private final AtomicInteger currentId;

		public FakeClientCallable(String clientIdentifier, int numberOfOperations, RasterEntry rasterEntry,
								  AtomicInteger currentId) {
			this.clientIdentifier = clientIdentifier;
			this.numberOfOperations = numberOfOperations;
			this.rasterEntry = rasterEntry;
			this.currentId = currentId;
		}

		/**
		 * @return the number of Ids that should be inside the {@link RasterEntry} for this {@link FakeClientCallable}.
		 */
		@Override
		public Set<ImmutablePair<String, Integer>> call() {
			List<ImmutablePair<String, Integer>> existingIds = new ArrayList<>();

			for (int i = 0; i < numberOfOperations; i++) {
				if (Utility.getTrueWithChance(70)) {
					int id = currentId.incrementAndGet();
					ImmutablePair<String, Integer> subscriptionId = ImmutablePair.of(clientIdentifier, id);
					rasterEntry.putSubscriptionId(subscriptionId);
					logger.trace("Added subscriptionId {}", subscriptionId);
					existingIds.add(subscriptionId);
				} else {
					if (!existingIds.isEmpty()) {
						ImmutablePair<String, Integer> subscriptionId =
								existingIds.get(Utility.randomInt(existingIds.size()));
						rasterEntry.removeSubscriptionId(subscriptionId);
						logger.trace("Removed subscriptionId {}", subscriptionId);
						existingIds.remove(subscriptionId);
					}
				}
			}

			logger.trace("Returning ids: {}", existingIds);
			return new HashSet<>(existingIds);
		}
	}

}