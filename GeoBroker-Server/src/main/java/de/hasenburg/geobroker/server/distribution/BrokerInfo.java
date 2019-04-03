package de.hasenburg.geobroker.server.distribution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.hasenburg.geobroker.commons.model.JSONable;

import java.util.Objects;

public class BrokerInfo implements JSONable {

	private final String brokerId;
	// This is the public address used by other brokers, so should be reachable via the internet
	private final String address;
	private final int port;

	@JsonCreator
	BrokerInfo(@JsonProperty("brokerId") String brokerId, @JsonProperty("address") String address,
			   @JsonProperty("port") int port) {
		this.brokerId = brokerId;
		this.address = address;
		this.port = port;
	}

	@Override
	public String toString() {
		return JSONable.toJSON(this);
	}

	/*****************************************************************
	 * Generated methods
	 ****************************************************************/

	public String getBrokerId() {
		return brokerId;
	}

	public String getAddress() {
		return address;
	}

	public int getPort() {
		return port;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		BrokerInfo that = (BrokerInfo) o;
		return port == that.port && brokerId.equals(that.brokerId) && address.equals(that.address);
	}

	@Override
	public int hashCode() {
		return Objects.hash(brokerId, address, port);
	}
}
