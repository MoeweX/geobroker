package de.hasenburg.geofencebroker.model.storage;

import de.hasenburg.geofencebroker.model.exceptions.RuntimeShapeException;
import de.hasenburg.geofencebroker.model.exceptions.RuntimeStorageException;
import de.hasenburg.geofencebroker.model.spatial.Geofence;
import de.hasenburg.geofencebroker.model.spatial.Location;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Raster {

	private static final Logger logger = LogManager.getLogger();

	private final ConcurrentHashMap<Location, RasterEntry> rasterEntries =
			new ConcurrentHashMap<>();

	protected final int granularity;

	/**
	 * Creates a new Raster. The index of each raster entry is the location of the entry's south west corner
	 *
	 * The raster always contains the whole world. Limitations to allowed subscriptions, e.g., the broker should only
	 * accept subscriptions from Europe, have to be made on another level
	 *
	 * The granularity is used to calculate the size of each {@link RasterEntry} in degrees. The size equals 1 degree /
	 * granularity
	 *
	 * @param granularity - must be >= 1
	 * @throws RuntimeStorageException if granularity < 1
	 */
	protected Raster(int granularity) {
		if (granularity < 1) {
			throw new RuntimeStorageException("Granularity must be >= 1, is " + granularity);
		}

		this.granularity = granularity;
	}

	protected int getNumberOfExistingRasterEntries() {
		return rasterEntries.size();
	}

	/**
	 * Adds a subscriptionId to the fitting {@link RasterEntry}s
	 *
	 * @param geofence - the geofence used to calculate the fitting {@link RasterEntry}
	 * @param subscriptionId - the subscriptionId to be added
	 */
	protected void addSubscriptionId(Geofence geofence, ImmutablePair<String, Integer> subscriptionId) {
		// TODO return current size
	}

	/**
	 * Removes a subscriptionId from the fitting {@link RasterEntry}s
	 *
	 * @param geofence - the geofence used to calculate the fitting {@link RasterEntry}
	 * @param subscriptionId - the subscriptionId to be removed
	 */
	protected void removeSubscriptionId(Geofence geofence, ImmutablePair<String, Integer> subscriptionId) {
		// TODO return current size
	}

	/**
	 * Returns all subscriptionIds that are in the fitting {@link RasterEntry}
	 *
	 * @param location - the location that determines which {@link RasterEntry} fits
	 */
	protected void getSubscriptionIdsForPublisherLocation(Location location) {
		// TODO add return type
	}


	/*****************************************************************
	 * Private methods
	 ****************************************************************/

	/**
	 * Calculates at what index the corresponding {@link RasterEntry} is stored
	 *
	 * @param location - the location
	 * @return - the index
	 */
	private Location calculateIndexLocation(Location location) {
		double latIndex = Math.floor(location.getLat() * granularity) / granularity;
		double lonIndex = Math.floor(location.getLon() * granularity) / granularity;

		return new Location(latIndex, lonIndex);
	}

	/**
	 * Calculates the indices of all {@link RasterEntry} the given geofence intersects with
	 *
	 * @param geofence - the geofence
	 * @return - a list of indices
	 */
	private List<Location> calculateIndexLocations(Geofence geofence) {

		return null;
	}

}
