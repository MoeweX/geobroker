FROM amazoncorretto:11

COPY out/GeoBroker-Server.jar GeoBroker-Server.jar
# needs to match supplied or default configuration
EXPOSE 5559

ENTRYPOINT ["java", "-jar", "GeoBroker-Server.jar"]
