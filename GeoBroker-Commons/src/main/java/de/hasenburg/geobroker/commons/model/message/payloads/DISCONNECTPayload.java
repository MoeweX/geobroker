package de.hasenburg.geobroker.commons.model.message.payloads;

import de.hasenburg.geobroker.commons.model.BrokerInfo;
import de.hasenburg.geobroker.commons.model.message.ReasonCode;

import java.util.Objects;

public class DISCONNECTPayload extends AbstractPayload {

	private ReasonCode reasonCode;
	private BrokerInfo brokerInfo;

	public DISCONNECTPayload() {

	}

	public DISCONNECTPayload(ReasonCode reasonCode) {
		super();
		this.reasonCode = reasonCode;
	}

	public DISCONNECTPayload(ReasonCode reasonCode, BrokerInfo brokerInfo) {
		super();
		this.reasonCode = reasonCode;
		this.brokerInfo = brokerInfo;
	}

	// overwrite as BrokerInfo might be null
	public boolean nullField() {
		return reasonCode == null;
	}

	/*****************************************************************
	 * Getter & Setter
	 ****************************************************************/

	public ReasonCode getReasonCode() {
		return reasonCode;
	}

	public BrokerInfo getBrokerInfo() {
		return brokerInfo;
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		DISCONNECTPayload that = (DISCONNECTPayload) o;
		return reasonCode == that.reasonCode && Objects.equals(brokerInfo, that.brokerInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(reasonCode, brokerInfo);
	}

}
