package de.hasenburg.geofencebroker.model;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.spatial4j.io.jackson.ShapesAsWKTModule;

import java.io.InputStream;
import java.util.Optional;

public interface JSONable {

	Logger logger = LogManager.getLogger();

	static <T> Optional<T> fromJSON(String json, Class<T> targetClass) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return Optional.of(mapper.readValue(json, targetClass));
		} catch (Exception e) {
			logger.error("Could not translate json to " + targetClass.getName(), e);
			return Optional.empty();
		}
	}
	
	static <T> Optional<T> fromJSON(InputStream stream, Class<T> targetClass) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return Optional.of(mapper.readValue(stream, targetClass));
		} catch (Exception e) {
			logger.error("Could not translate json to " + targetClass.getName(), e);
			return Optional.empty();
		}
	}

	static String toJSON(JSONable obj) {
		return JSONable.toJSON(obj, true);
	}

	static String toJSON(JSONable obj, boolean indent) {
		// Only include non-null field
		ObjectMapper mapper = new ObjectMapper()
				.setSerializationInclusion(Include.NON_NULL)
				.setSerializationInclusion(Include.NON_EMPTY);
		mapper.registerModule(new Jdk8Module());
		if (indent) {
			mapper.enable(SerializationFeature.INDENT_OUTPUT);
		}

		String json = null;
		try {
			json = mapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			logger.error("Could not create JSON from object", e);
		}
		return json;
	}

	@SuppressWarnings({"ConstantConditions", "unchecked"})
	static <T> T clone(JSONable object) {
		if (object == null) {
			return null;
		}
		
		String json = toJSON(object);
		JSONable clone = fromJSON(json, object.getClass()).get();
		
		return (T) clone;
	}
	
}
