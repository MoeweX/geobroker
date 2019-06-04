package de.hasenburg.geobroker.commons.model.message;

import java.util.Objects;

public class Topic {

	public static final String TOPIC_LEVEL_SEPARATOR = "/";

	private final String topic;
	private final String[] levelSpecifiers;

	public Topic(String topic) {
		this.topic = topic;
		this.levelSpecifiers = topic.split(TOPIC_LEVEL_SEPARATOR);
	}

	public String getLevelSpecifier(int levelIndex) {
		if (levelIndex >= getNumberOfLevels()) {
			throw new RuntimeException(
					"Look what you did, you killed the server by not checking the number of levels beforehand!");
		}
		return levelSpecifiers[levelIndex];
	}

	/**
	 * @return the number of available levels
	 */
	public int getNumberOfLevels() {
		return levelSpecifiers.length;
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	public String getTopic() {
		return topic;
	}

	public String[] getLevelSpecifiers() {
		return levelSpecifiers;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof Topic)) {
			return false;
		}
		Topic topic1 = (Topic) o;
		return Objects.equals(getTopic(), topic1.getTopic());
	}

	@Override
	public int hashCode() {

		return Objects.hash(getTopic());
	}

	@Override
	public String toString() {
		return getTopic();
	}
}
