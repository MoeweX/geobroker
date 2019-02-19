package de.hasenburg.geolife;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.moandjiezana.toml.Toml;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;

public class ClientConfiguration {

	private static final Logger logger = LogManager.getLogger();

	private String managerName = "DefaultManager";

	// server machine properties
	private String address;
	private int port;

	// client manager properties
	private Integer runtime; // in min
	private Integer offset; // in milli-sec
	private Integer index;
	private Integer count;

	// client properties
	private Double geofenceSize; // in degree
	private Integer payloadSize; // in byte
	protected final static String S3_BUCKET_NAME = "geobroker";
	protected final static String S3_CONFIGURATIONS_FOLDER = "Configurations/";
	protected final static String S3_GEOLIFE_FOLDER = "Geolife/";

	/**
	 * Fails fast in case properties cannot be found
	 *
	 * @param configurationName
	 * @return the configuration for the client manager
	 */
	public static ClientConfiguration readConfigurationFromS3(String configurationName, String managerName) {
		ClientConfiguration c = new ClientConfiguration();
		c.managerName = managerName;

		AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

		S3Object o = s3.getObject(S3_BUCKET_NAME, S3_CONFIGURATIONS_FOLDER + configurationName);
		Toml toml = new Toml().read(new InputStreamReader(o.getObjectContent()));

		Toml serverMachine = toml.getTable("serverMachine");
		c.address = serverMachine.getString("address");
		c.port = Math.toIntExact(serverMachine.getLong("port"));

		Toml clientManager = toml.getTable("clientManager");
		c.runtime = Math.toIntExact(clientManager.getLong("runtime"));
		c.offset = Math.toIntExact(clientManager.getLong("offset"));

		Toml clientManagerSpecifics = clientManager.getTable(managerName);
		c.index = Math.toIntExact(clientManagerSpecifics.getLong("index"));
		c.count = Math.toIntExact(clientManagerSpecifics.getLong("count"));

		Toml client = toml.getTable("client");
		c.geofenceSize = client.getDouble("geofenceSize");
		c.payloadSize = Math.toIntExact(client.getLong("payloadSize"));

		try {
			logger.info(c.toString());
		} catch (NullPointerException e) {
			logger.fatal("ClientConfiguration is missing mandatory fields", e);
		}

		return c;
	}

	public static void main(String[] args) {
		ClientConfiguration c = ClientConfiguration.readConfigurationFromS3("example_config.toml", "Weihnachtsmann");
	}

	/*****************************************************************
	 * Generated methods
	 ****************************************************************/

	public String getManagerName() {
		return managerName;
	}

	public String getAddress() {
		return address;
	}

	public int getPort() {
		return port;
	}

	public Integer getRuntime() {
		return runtime;
	}

	public Integer getOffset() {
		return offset;
	}

	public Integer getIndex() {
		return index;
	}

	public Integer getCount() {
		return count;
	}

	public Double getGeofenceSize() {
		return geofenceSize;
	}

	public Integer getPayloadSize() {
		return payloadSize;
	}

	@Override
	public String toString() {
		return "ClientConfiguration{" + "managerName='" + managerName + '\'' + ", address='" + address + '\'' + ", port=" + port +
				", runtime=" + runtime + ", offset=" + offset + ", index=" + index + ", count=" + count +
				", geofenceSize=" + geofenceSize + ", payloadSize=" + payloadSize + '}';
	}
}
