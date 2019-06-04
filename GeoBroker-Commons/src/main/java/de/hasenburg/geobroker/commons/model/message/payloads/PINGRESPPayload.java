package de.hasenburg.geobroker.commons.model.message.payloads;

import de.hasenburg.geobroker.commons.model.message.ReasonCode;

import java.util.Objects;

public class PINGRESPPayload extends AbstractPayload {

	protected ReasonCode reasonCode;

	public PINGRESPPayload(ReasonCode reasonCode) {
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
		if (!(o instanceof PINGRESPPayload)) {
			return false;
		}
		PINGRESPPayload that = (PINGRESPPayload) o;
		return getReasonCode() == that.getReasonCode();
	}

	@Override
	public int hashCode() {

		return Objects.hash(getReasonCode());
	}
}
