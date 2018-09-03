package de.hasenburg.geofencebroker.model.storage;

import de.hasenburg.geofencebroker.model.spatial.Geofence;
import de.hasenburg.geofencebroker.model.spatial.Location;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Raster {

	private static final Logger logger = LogManager.getLogger();

	private final ConcurrentHashMap<Location, RasterEntry> rasterEntries = new ConcurrentHashMap<>();

	/**
	 * Create a new Raster
	 *
	 * TODO: might make sense to define height and width in km so that all squares have the same size
	 *
	 * @param dimensions - the raster dimensions described by a {@link Geofence}. Must be a square.
	 * @param height - the height of each {@link RasterEntry} in degree
	 * @param width - the width of each {@link RasterEntry} in degree
	 */
	public Raster(Geofence dimensions, double height, double width) {

	}

	/**
	 * Adds a subscriptionId to the fitting {@link RasterEntry}
	 *
	 * @param geofence - the geofence used to calculate the fitting {@link RasterEntry}
	 * @param subscriptionId - the subscriptionId to be added
	 */
	public void addSubscriptionId(Geofence geofence, ImmutablePair<String, Integer> subscriptionId) {
		// TODO return current size
	}

	/**
	 * Removes a subscriptionId from the fitting {@link RasterEntry}
	 *
	 * @param geofence - the geofence used to calculate the fitting {@link RasterEntry}
	 * @param subscriptionId - the subscriptionId to be removed
	 */
	public void removeSubscriptionId(Geofence geofence, ImmutablePair<String, Integer> subscriptionId) {
		// TODO return current size
	}

	/**
	 * Returns all subscriptionIds that in the fitting {@link RasterEntry}
	 *
	 * @param location - the location that determines which {@link RasterEntry} fits
	 */
	public void getSubscriptionIdsForPublisherLocation(Location location) {
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

		return null;
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
