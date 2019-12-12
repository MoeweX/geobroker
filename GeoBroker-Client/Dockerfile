FROM amazoncorretto:11

COPY out/MultiFileClient.jar MultiFileClient.jar

# uses 80% of the memory available to the container
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=80.0", "-XshowSettings:vm", "-jar", "MultiFileClient.jar"]
