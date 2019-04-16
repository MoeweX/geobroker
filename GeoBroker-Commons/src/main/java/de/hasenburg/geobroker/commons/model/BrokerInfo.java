package de.hasenburg.geobroker.commons.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Stores information of a given broker: brokerId, publicly reachable server ip and server port.
 */
public class BrokerInfo implements JSONable {

	private final String brokerId;
	// This is the public ip used by other brokers, so should be reachable via the internet
	private final String ip;
	private final int port;

	@JsonCreator
	public BrokerInfo(@JsonProperty("brokerId") String brokerId, @JsonProperty("ip") String ip,
					  @JsonProperty("port") int port) {
		this.brokerId = brokerId;
		this.ip = ip;
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

	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		BrokerInfo that = (BrokerInfo) o;
		return port == that.port && brokerId.equals(that.brokerId) && ip.equals(that.ip);
	}

	@Override
	public int hashCode() {
		return Objects.hash(brokerId, ip, port);
	}
}
