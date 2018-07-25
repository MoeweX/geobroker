package de.hasenburg.geofencebroker.model.payload;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize
public class CONNECTPayload  extends AbstractPayload {

	public CONNECTPayload() {

	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof CONNECTPayload;
	}
}
