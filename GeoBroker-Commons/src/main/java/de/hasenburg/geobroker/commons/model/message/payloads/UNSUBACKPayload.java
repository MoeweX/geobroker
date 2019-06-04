package de.hasenburg.geobroker.commons.model.message.payloads;

import de.hasenburg.geobroker.commons.model.message.ReasonCode;

import java.util.Objects;

public class UNSUBACKPayload extends AbstractPayload {

	protected ReasonCode reasonCode;

	public UNSUBACKPayload(ReasonCode reasonCode) {
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
		if (!(o instanceof UNSUBACKPayload)) {
			return false;
		}
		UNSUBACKPayload that = (UNSUBACKPayload) o;
		return getReasonCode() == that.getReasonCode();
	}

	@Override
	public int hashCode() {

		return Objects.hash(getReasonCode());
	}
}
