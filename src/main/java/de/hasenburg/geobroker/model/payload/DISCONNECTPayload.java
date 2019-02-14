package de.hasenburg.geobroker.model.payload;

import de.hasenburg.geobroker.communication.ReasonCode;

import java.util.Objects;

public class DISCONNECTPayload extends AbstractPayload {

	protected ReasonCode reasonCode;

	public DISCONNECTPayload() {

	}

	public DISCONNECTPayload(ReasonCode reasonCode) {
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
		if (!(o instanceof DISCONNECTPayload)) {
			return false;
		}
		DISCONNECTPayload that = (DISCONNECTPayload) o;
		return getReasonCode() == that.getReasonCode();
	}

	@Override
	public int hashCode() {

		return Objects.hash(getReasonCode());
	}
}
