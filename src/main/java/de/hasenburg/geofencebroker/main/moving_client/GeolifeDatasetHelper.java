package de.hasenburg.geofencebroker.main.moving_client;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeolifeDatasetHelper {

	private static final Logger logger = LogManager.getLogger();

	private AmazonS3 s3;
	private Map<Integer, String> indices = new HashMap<>();

	public GeolifeDatasetHelper() {
		s3 = AmazonS3ClientBuilder.defaultClient();
	}

	public void prepare() {
		try {
			// read in indices file
			S3Object o = s3.getObject(Configuration.S3_BUCKET_NAME, keyIndices());
			BufferedReader reader = new BufferedReader(new InputStreamReader(o.getObjectContent()));
			String line = reader.readLine();

			while (line != null && !line.equals("")) {
				String[] elements = line.split(";");
				indices.put(Integer.parseInt(elements[0]), elements[1]);
				line = reader.readLine();
			}

			if (indices.size() != 18670) {
				logger.fatal("Indices file did not contain the correct amount of elements, was " + indices.size());
			} else {
				logger.info("Successfully read the 18670 geolife dataset index entries");
			}

		} catch (IOException | NullPointerException e) {
			logger.fatal("Could not read indices file", e);
			System.exit(1);
		}
	}

	public List<String> getFileContentAtIndex(int index) {
		// TODO NEXT STEP
		return null;
	}

	/*****************************************************************
	 * S3 Keymanager
	 ****************************************************************/

	private String keyIndices() {
		return Configuration.S3_GEOLIFE_FOLDER + "indices.csv";
	}

	private String keyRoute(String relativePath) {
		return Configuration.S3_GEOLIFE_FOLDER + relativePath;
	}

	public static void main (String[] args) {
	    GeolifeDatasetHelper gdh = new GeolifeDatasetHelper();
	    gdh.prepare();
	    logger.info(gdh.indices.get(10));
	}

}
