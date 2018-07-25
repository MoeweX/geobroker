package de.hasenburg.geofencebroker.model.payload;

import de.hasenburg.geofencebroker.communication.ReasonCode;

import java.util.Objects;

public class CONNACKPayload extends AbstractPayload {

	protected ReasonCode reasonCode;

	public CONNACKPayload() {

	}

	public CONNACKPayload(ReasonCode reasonCode) {
		super();
		this.reasonCode = reasonCode;
	}

	/*****************************************************************
	 * Getter & Setter
	 ****************************************************************/

	public ReasonCode getReasonCode() {
		return reasonCode;
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof CONNACKPayload)) {
			return false;
		}
		CONNACKPayload that = (CONNACKPayload) o;
		return getReasonCode() == that.getReasonCode();
	}

	@Override
	public int hashCode() {

		return Objects.hash(getReasonCode());
	}
}
