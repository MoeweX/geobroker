package de.hasenburg.geobroker.server.main;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.moandjiezana.toml.Toml;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStreamReader;

public class Configuration {

	private static final Logger logger = LogManager.getLogger();

	private Integer granularity = 1;
	private Integer messageProcessors = 1;

	private final static String S3_BUCKET_NAME = "geobroker";
	private final static String S3_CONFIGURATIONS_FOLDER = "Configurations/";

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
	public static Configuration readDefaultConfiguration() {
		try {
			Configuration c = new Configuration();

			Toml toml = new Toml().read(Configuration.class.getClassLoader().getResourceAsStream("configuration.toml"));

			return parseToml(c, toml);
		} catch (Exception e) {
			logger.fatal("Could not read default configuration", e);
		}
		System.exit(1);
		return null; // WHY DO I NEED YOU?
	}

	private static Configuration parseToml(Configuration c, Toml toml) {
		Toml server = toml.getTable("server");
		c.granularity = Math.toIntExact(server.getLong("granularity", 1L));
		c.messageProcessors = Math.toIntExact(server.getLong("messageProcessors", 1L));
		try {
			logger.info(c.toString());
		} catch (NullPointerException e) {
			logger.fatal("Configuration is missing mandatory fields", e);
		}

		return c;
	}

	public static void main(String[] args) {
		Configuration c = Configuration.readConfigurationFromS3("example_config.toml");
		Configuration c2 = Configuration.readDefaultConfiguration();
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

	@Override
	public String toString() {
		return "Configuration{" + "granularity=" + granularity + ", messageProcessors=" + messageProcessors + '}';
	}
}
