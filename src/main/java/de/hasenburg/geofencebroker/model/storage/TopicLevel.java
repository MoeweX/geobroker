package de.hasenburg.geofencebroker.model.storage;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link TopicLevel} is a single part of a complete {@link de.hasenburg.geofencebroker.model.Topic}. For example, the
 * topic a/b/c has the three topic levels a, b, and c. Topic levels can also be single level wildcards {@link
 * TopicLevel#SINGLE_LEVEL_WILDCARD} or multilevel wildcards {@link TopicLevel#MULTI_LEVEL_WILDCARD}.
 */
public class TopicLevel {

	public static final String SINGLE_LEVEL_WILDCARD = "+";
	public static final String MULTI_LEVEL_WILDCARD = "#";

	private final String levelSpecifier;
	private final Raster raster;
	// levelSpecifier -> TopicLevel
	private final ConcurrentHashMap<String, TopicLevel> children = new ConcurrentHashMap<>();

	protected TopicLevel(String levelSpecifier, int granularity) {
		this.levelSpecifier = levelSpecifier; raster = new Raster(granularity);
	}

	protected TopicLevel getOrCreateChild(String levelSpecifier) {
		return children.computeIfAbsent(levelSpecifier, k -> new TopicLevel(levelSpecifier, raster.granularity));
	}

	protected Collection<TopicLevel> getAllChildren() {
		return Collections.unmodifiableCollection(children.values());
	}

	/*****************************************************************
	 * Generated methods
	 ****************************************************************/

	public String getLevelSpecifier() {
		return levelSpecifier;
	}

	public Raster getRaster() {
		return raster;
	}
}
