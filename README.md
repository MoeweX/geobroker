# GeoBroker

GeoBroker is a pub/sub system research prototype that not only uses content information, i.e., the topic of a message, 
but also the geo-context of publishers and subscribers for the matching of messages.

As the project requires some new features added to ZeroMQ and spatial4j which are not released yet to mvn central, 
one needs to `mvn install` these projects first:
```
<dependency>
    <groupId>org.zeromq</groupId>
    <artifactId>jeromq</artifactId>
    <version>0.4.4-SNAPSHOT</version>
</dependency>

<dependency>
    <groupId>org.locationtech.spatial4j</groupId>
    <artifactId>spatial4j</artifactId>
    <version>0.8-SNAPSHOT</version>
</dependency>
```

To start the Broker, run the main method of *de.hasenburg.geobroker.main.Broker.java*. 
A very simple client can be started by running *de.hasenburg.geobroker.main.SimpleClient.java*.