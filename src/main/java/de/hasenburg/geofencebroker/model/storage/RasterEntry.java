package de.hasenburg.geofencebroker.model.storage;

import de.hasenburg.geofencebroker.model.spatial.Geofence;
import de.hasenburg.geofencebroker.model.spatial.Location;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RasterEntry {

	private final Location index;
	private final Geofence rasterEntryBox; // let's buffer this

	private final ConcurrentHashMap<String, Set<ImmutablePair<String, Integer>>> existingSubscriptionIds =
			new ConcurrentHashMap<>();
	private final AtomicInteger numSubscriptionIds = new AtomicInteger(0);

	public RasterEntry(Location index, int granularity) {
		this.index = index;
		this.rasterEntryBox = Geofence.polygon(Arrays.asList(index,
															 // south west
															 new Location(index.getLat() + granularity, index.getLon()),
															 // north west
															 new Location(index.getLat() + granularity,
																		  index.getLon() + granularity),
															 // north east
															 new Location(index.getLat(), index.getLon() + granularity)
															 // south east
		));
	}

	/*****************************************************************
	 * Subscribe/Unsubscribe Operations
	 ****************************************************************/

	/**
	 * Puts the given subscriptionId to the {@link RasterEntry}.
	 *
	 * It is assumed that every subscriptionId is unique. Otherwise, inconsistencies may arise.
	 *
	 * @param subscriptionId - unique identifier for a subscription that comprises a clientId and an integer
	 * @return the number of subscriptionIds stored in the {@link RasterEntry} after the operation completed
	 */
	protected int putSubscriptionId(ImmutablePair<String, Integer> subscriptionId) {
		// get the set or create a new one and place in map
		Set<ImmutablePair<String, Integer>> set =
				existingSubscriptionIds.computeIfAbsent(subscriptionId.left, k -> ConcurrentHashMap.newKeySet());
		// add integer part of id
		set.add(subscriptionId);
		return numSubscriptionIds.incrementAndGet();
	}

	/**
	 * Removes the given subscriptionId from the {@link RasterEntry}.
	 *
	 * It is assumed that every subscriptionId is unique. Otherwise, inconsistencies may arise.
	 *
	 * @param subscriptionId - unique identifier for a subscription that comprises a clientId and an integer
	 * @return the number of subscriptionIds stored in the {@link RasterEntry} after the operation completed
	 */
	protected int removeSubscriptionId(ImmutablePair<String, Integer> subscriptionId) {
		Set<ImmutablePair<String, Integer>> set = existingSubscriptionIds.get(subscriptionId.left);
		if (set != null && set.remove(subscriptionId)) {
			// if the client has entries + the id is part of the client's entries
			return numSubscriptionIds.decrementAndGet();
		}
		return numSubscriptionIds.get();
	}

	/*****************************************************************
	 * Process Published Message Operations
	 ****************************************************************/

	/**
	 * Returns all subscriptionIds for a given client.
	 *
	 * NOTE: for performance reason, this method returns a reference to the internal set of {@link RasterEntry}, so do
	 * not update it! Instead, use {@link #putSubscriptionId(ImmutablePair)} and {@link
	 * #removeSubscriptionId(ImmutablePair)}.
	 *
	 * @param clientId - the clientId
	 * @return the specified set
	 */
	public Set<ImmutablePair<String, Integer>> getSubscriptionIdsForClientIdentifier(String clientId) {
		return Collections.unmodifiableSet(existingSubscriptionIds.get(clientId));
	}

	/**
	 * Returns all subscriptions.
	 *
	 * NOTE: for performance reason, this method returns a reference to the internal map of {@link RasterEntry}, so do
	 * not update it! Instead, use {@link #putSubscriptionId(ImmutablePair)} and {@link
	 * #removeSubscriptionId(ImmutablePair)}.
	 *
	 * @return all subscriptions
	 */
	public Map<String, Set<ImmutablePair<String, Integer>>> getAllSubscriptionIds() {
		// it would be best to also make the set immutable, but this were a big performance loss
		// -> only the map and the set's values are immutable
		return Collections.unmodifiableMap(existingSubscriptionIds);
	}

	/*****************************************************************
	 * Getters
	 ****************************************************************/

	protected Integer getNumSubscriptionIds() {
		return numSubscriptionIds.get();
	}

	protected Location getIndex() {
		return index;
	}

	protected Geofence getRasterEntryBox() {
		return rasterEntryBox;
	}

}
