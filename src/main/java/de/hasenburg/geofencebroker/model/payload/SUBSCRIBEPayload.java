package de.hasenburg.geofencebroker.model.payload;

import de.hasenburg.geofencebroker.model.Topic;
import de.hasenburg.geofencebroker.model.spatial.Geofence;

import java.util.Objects;

public class SUBSCRIBEPayload extends AbstractPayload {

	protected Topic topic;
	protected Geofence geofence;

	public SUBSCRIBEPayload() {

	}

	public SUBSCRIBEPayload(Topic topic, Geofence geofence) {
		super();
		this.topic = topic;
		this.geofence = geofence;
	}

	/*****************************************************************
	 * Getter & Setter
	 ****************************************************************/

	public Topic getTopic() {
		return topic;
	}

	public Geofence getGeofence() {
		return geofence;
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof SUBSCRIBEPayload)) {
			return false;
		}
		SUBSCRIBEPayload that = (SUBSCRIBEPayload) o;
		return Objects.equals(getTopic(), that.getTopic()) &&
				Objects.equals(getGeofence(), that.getGeofence());
	}

	@Override
	public int hashCode() {

		return Objects.hash(getTopic(), getGeofence());
	}
}
