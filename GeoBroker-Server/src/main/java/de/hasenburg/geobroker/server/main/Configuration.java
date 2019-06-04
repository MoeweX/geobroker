package de.hasenburg.geobroker.server.main;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.moandjiezana.toml.Toml;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;

public class Configuration {

	private static final Logger logger = LogManager.getLogger();

	enum Mode {
		single, disgb_subscriberMatching, disgb_publisherMatching
	}

	private final static String S3_BUCKET_NAME = "geobroker";
	private final static String S3_CONFIGURATIONS_FOLDER = "Configurations/";

	private String brokerId = "broker";
	private Integer port = 5559;
	private Integer granularity = 1;
	private Integer messageProcessors = 1;

	private Mode mode = Mode.single;

	// disgb specific
	private String brokerAreaFilePath = "defaultBrokerAreas.json";
	private Integer brokerCommunicators = 1;

	public Configuration() {
		// default values
	}

	public Configuration(int granularity, int messageProcessors) {
		this.granularity = granularity;
		this.messageProcessors = messageProcessors;
	}

	public static Configuration readConfigurationFromS3(String configurationName) {
		Configuration c = new Configuration();

		AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

		S3Object o = s3.getObject(S3_BUCKET_NAME, S3_CONFIGURATIONS_FOLDER + configurationName);
		Toml toml = new Toml().read(new InputStreamReader(o.getObjectContent()));

		return parseToml(c, toml);
	}

	/**
	 * Create a new configuration.
	 */
	public static Configuration readConfiguration(String filePath) {
		try {
			Configuration c = new Configuration();
			File f = new File(filePath);

			Toml toml = new Toml().read(f);

			return parseToml(c, toml);
		} catch (Exception e) {
			logger.fatal("Could not configuration", e);
		}
		System.exit(1);
		return null; // WHY DO I NEED YOU?
	}

	/**
	 * Create a new configuration.
	 */
	public static Configuration readInternalConfiguration(String filePath) {
		try {
			Configuration c = new Configuration();

			//noinspection ConstantConditions as exception catches nullpointers
			Toml toml = new Toml().read(Configuration.class.getClassLoader().getResourceAsStream(filePath));

			return parseToml(c, toml);
		} catch (Exception e) {
			logger.fatal("Could not internal configuration", e);
		}
		System.exit(1);
		return null; // WHY DO I NEED YOU?
	}

	private static Configuration parseToml(Configuration c, Toml toml) {

		// server information
		Toml toml_server = toml.getTable("server");
		c.brokerId = toml_server.getString("brokerId", c.brokerId);
		c.port = Math.toIntExact(toml_server.getLong("port", c.port.longValue()));

		c.granularity = Math.toIntExact(toml_server.getLong("granularity", c.granularity.longValue()));
		c.messageProcessors = Math.toIntExact(toml_server.getLong("messageProcessors",
				c.messageProcessors.longValue()));

		// server mode specific information
		Toml toml_server_mode = toml_server.getTable("mode");
		if (toml_server_mode != null) {
			c.mode = Mode.valueOf(toml_server_mode.getString("name", Mode.single.toString()));

			// disgb specific
			c.brokerAreaFilePath = toml_server_mode.getString("brokerAreaFilePath", c.brokerAreaFilePath);
			c.brokerCommunicators = Math.toIntExact(toml_server_mode.getLong("brokerCommunicators",
					c.brokerCommunicators.longValue()));
		}

		return c;
	}

	public static void main(String[] args) {
		Configuration c = Configuration.readInternalConfiguration("configuration.toml");
		logger.info(c);
	}

	/*****************************************************************
	 * Generated methods
	 ****************************************************************/

	public Integer getGranularity() {
		return granularity;
	}

	public Integer getMessageProcessors() {
		return messageProcessors;
	}

	public String getBrokerId() {
		return brokerId;
	}

	public Integer getPort() {
		return port;
	}

	public Mode getMode() {
		return mode;
	}

	/**
	 * This field only has a meaning when {@link #getMode()} == Mode.disgb.
	 */
	public String getBrokerAreaFilePath() {
		return brokerAreaFilePath;
	}

	/**
	 * This field only has a meaning when {@link #getMode()} == Mode.disgb.
	 */
	public Integer getBrokerCommunicators() {
		return brokerCommunicators;
	}

	@Override
	public String toString() {
		return "Configuration{" + "brokerId='" + brokerId + '\'' + ", port=" + port + ", granularity=" + granularity +
				", messageProcessors=" + messageProcessors + ", mode=" + mode + ", brokerAreaFilePath='" +
				brokerAreaFilePath + '\'' + ", brokerCommunicators=" + brokerCommunicators + '}';
	}
}
