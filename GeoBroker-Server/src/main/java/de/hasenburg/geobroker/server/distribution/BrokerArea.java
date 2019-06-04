package de.hasenburg.geobroker.server.distribution;

import de.hasenburg.geobroker.commons.model.BrokerInfo;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;

import java.util.Objects;

// TODO migrate to commons and add serialization to kryo serializer
public class BrokerArea {

	private final BrokerInfo responsibleBroker;
	private final Geofence coveredArea;

	public BrokerArea(BrokerInfo responsibleBroker, Geofence coveredArea) {
		this.responsibleBroker = responsibleBroker;
		this.coveredArea = coveredArea;
	}

	BrokerInfo getResponsibleBroker() {
		return responsibleBroker;
	}

	Geofence getCoveredArea() {
		return coveredArea;
	}

	boolean CheckResponsibleBroker(String brokerId) {
		return responsibleBroker.getBrokerId().equals(brokerId);
	}

	boolean ContainsLocation(Location location) {
		return coveredArea.contains(location);
	}

	boolean intersects(Geofence messageGeofence) {
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
