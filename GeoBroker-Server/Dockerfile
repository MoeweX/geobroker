FROM amazoncorretto:11

COPY out/GeoBroker-Server.jar GeoBroker-Server.jar
# needs to match supplied or default configuration
EXPOSE 5559

# uses 80% of the memory available to the container
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=80.0", "-XshowSettings:vm", "-jar", "GeoBroker-Server.jar"]
