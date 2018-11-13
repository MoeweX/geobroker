package de.hasenburg.geofencebroker.main;

import com.moandjiezana.toml.Toml;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Configuration {

	private static final Logger logger = LogManager.getLogger();

	private int granularity = 1;

	/**
	 * Create a new configuration.
	 */
	public static Configuration readDefaultConfiguration() {
		try {
			Configuration c = new Configuration();

			Toml toml = new Toml().read(Configuration.class.getClassLoader().getResourceAsStream("toml.properties"));

			Toml rasterToml = toml.getTable("raster");
			c.granularity = Math.toIntExact(toml.getLong("granularity", 1L));

			return c;
		} catch (Exception e) {
			logger.fatal("Cloud not read default configuration", e);
		}
		System.exit(1);
		return null; // WHY DO I NEED YOU?
	}

	/*****************************************************************
	 * Generated methods
	 ****************************************************************/

	public int getGranularity() {
		return granularity;
	}
}
