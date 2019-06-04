package de.hasenburg.geobroker.commons.model;

import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;

import java.util.Objects;

public class BrokerArea {

	private final BrokerInfo responsibleBroker;
	private final Geofence coveredArea;

	public BrokerArea(BrokerInfo responsibleBroker, Geofence coveredArea) {
		this.responsibleBroker = responsibleBroker;
		this.coveredArea = coveredArea;
	}

	public BrokerInfo getResponsibleBroker() {
		return responsibleBroker;
	}

	public Geofence getCoveredArea() {
		return coveredArea;
	}

	public boolean CheckResponsibleBroker(String brokerId) {
		return responsibleBroker.getBrokerId().equals(brokerId);
	}

	public boolean ContainsLocation(Location location) {
		return coveredArea.contains(location);
	}

	public boolean intersects(Geofence messageGeofence) {
		return coveredArea.intersects(messageGeofence);
	}

	/*****************************************************************
	 * Generated methods
	 ****************************************************************/

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		BrokerArea that = (BrokerArea) o;
		return responsibleBroker.equals(that.responsibleBroker) && coveredArea.equals(that.coveredArea);
	}

	@Override
	public int hashCode() {
		return Objects.hash(responsibleBroker, coveredArea);
	}

}
