package de.hasenburg.geofencebroker.model.storage;

import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RasterEntry {

	private final String topicPart;
	private final ConcurrentHashMap<String, Set<Integer>> existingSubscriptionIds = new ConcurrentHashMap<>();
	private final AtomicInteger numSubscriptionIds = new AtomicInteger(0);

	public RasterEntry(String topicPart) {
		this.topicPart = topicPart;
	}

	/**
	 * Adds the given subscriptionId to the {@link RasterEntry}.
	 *
	 * It is assumed that every subscriptionId is unique and also added ONCE only. Otherwise, inconsistencies may arise.
	 *
	 * @param subscriptionId - unique identifier for a subscription that comprises a clientId and an integer
	 * @return the number of subscriptionIds stored in the {@link RasterEntry} after the operation completed
	 */
	public int addSubscriptionId(ImmutablePair<String, Integer> subscriptionId) {
		// get the set or create a new one and place in map
		Set<Integer> set = existingSubscriptionIds.computeIfAbsent(subscriptionId.left, k -> ConcurrentHashMap.newKeySet());
		// add integer part of id
		set.add(subscriptionId.right);
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
	public int removeSubscriptionId(ImmutablePair<String, Integer> subscriptionId) {
		Set<Integer> set = existingSubscriptionIds.get(subscriptionId.left);
		if (set != null && set.remove(subscriptionId.right)) {
			// if the client has entries + the id is part of the client's entries
			return numSubscriptionIds.decrementAndGet();
		}
		return numSubscriptionIds.get();
	}

	/**
	 * Returns a set of integers that together with the given clientId represent the subscriptionsIds for the given client.
	 *
	 * @param clientId - the clientId
	 * @return the specified set
	 */
	public Set<Integer> getSubscriptionIdsForClientIdentifier(String clientId) {
		return existingSubscriptionIds.get(clientId);
	}

	/*****************************************************************
	 * Generated methods
	 ****************************************************************/

	public String getTopicPart() {
		return topicPart;
	}

	public Integer getNumSubscriptionIds() {
		return numSubscriptionIds.get();
	}
}
