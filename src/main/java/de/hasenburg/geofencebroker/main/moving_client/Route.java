package de.hasenburg.geofencebroker.main.moving_client;

import de.hasenburg.geofencebroker.model.spatial.Location;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.List;

public class Route {

	private List<ImmutablePair<Location, Integer>> visitedLocations;

	/**
	 * Add a location to the visited locations list.
	 *
	 * @param location - to be added location
	 * @param requiredTravelTime - travel time needed to reach the location
	 */
	public void addLocation(Location location, Integer requiredTravelTime) {
		visitedLocations.add(ImmutablePair.of(location, requiredTravelTime));
	}

	public List<ImmutablePair<Location, Integer>> getVisitedLocations() {
		return this.visitedLocations;
	}

}
