package de.hasenburg.geofencebroker.model.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link TopicLevel} is a single part of a complete {@link de.hasenburg.geofencebroker.model.Topic}. For example, the
 * topic a/b/c has the three topic levels a, b, and c. Topic levels can also be single level wildcards {@link
 * TopicLevel#SINGLE_LEVEL_WILDCARD} or multilevel wildcards {@link TopicLevel#MULTI_LEVEL_WILDCARD}.
 */
public class TopicLevel {

	private static final Logger logger = LogManager.getLogger();

	public static final String SINGLE_LEVEL_WILDCARD = "+";
	public static final String MULTI_LEVEL_WILDCARD = "#";

	private final String levelSpecifier;
	private final Raster raster;
	// levelSpecifier -> TopicLevel
	private final ConcurrentHashMap<String, TopicLevel> children = new ConcurrentHashMap<>();

	protected TopicLevel(String levelSpecifier, int granularity) {
		this.levelSpecifier = levelSpecifier;
		raster = new Raster(granularity);
	}

	/*****************************************************************
	 * Subscribe/Unsubscribe Operations
	 ****************************************************************/

	/**
	 * Gets an already existing child for the given level specifiers. A minimum of one specifier must be provided. If at
	 * one point none exist yet, it and all subsequent ones will be created.
	 *
	 * @param levelSpecifiers - the given level specifiers
	 * @return the child
	 */
	protected TopicLevel getOrCreateChildren(String... levelSpecifiers) {
		TopicLevel currentChild = this;
		for (String specifier : levelSpecifiers) {
			currentChild = currentChild.children
					.computeIfAbsent(specifier, k -> new TopicLevel(specifier, raster.granularity));
		}

		return currentChild;
	}

	/**
	 * Gets an already existing child for the given level specifiers. A minimum of one specifier must be provided. If at
	 * one point none exist yet, this method returns null
	 *
	 * @param levelSpecifiers - the given level specifiers
	 * @return the child or null
	 */
	protected TopicLevel getChildren(String... levelSpecifiers) {
		TopicLevel currentChild = this;
		for (String specifier : levelSpecifiers) {
			currentChild = currentChild.children.get(specifier);
			if (currentChild == null) {
				return null;
			}
		}
		return currentChild;
	}

	/*****************************************************************
	 * Process Published Message Operations
	 ****************************************************************/

	protected TopicLevel getDirectChild(String levelSpecifier) {
		return children.get(levelSpecifier);
	}

	protected Collection<TopicLevel> getAllDirectChildren() {
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
