package de.hasenburg.geofencebroker.model.storage;

import de.hasenburg.geofencebroker.model.spatial.Geofence;
import de.hasenburg.geofencebroker.model.spatial.Location;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

public class Raster {

	private static final Logger logger = LogManager.getLogger();

	private final ConcurrentHashMap<Location, RasterEntry> rasterEntries = new ConcurrentHashMap<>();

	public void addSubscriptionId(Geofence geofence, ImmutablePair<String, Integer> subscriptionId) {

	}

	public void removeSubscriptionId(Geofence geofence, ImmutablePair<String, Integer> subscriptionId) {

	}

	public void getSubscriptionIdsForPublisherLocation(Location location) {

	}

}
