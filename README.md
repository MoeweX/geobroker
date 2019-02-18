# GeoBroker

GeoBroker is a pub/sub system research prototype that not only uses content information, i.e., the topic of a message, 
but also the geo-context of publishers and subscribers for the matching of messages.

As this project contains multiple git submodules, one needs to run the following after cloning:
```
git submodule init
git submodule update
```

To start the Broker, run the main method of *de.hasenburg.geobroker.main.Broker.java*. 
A very simple client can be started by running *de.hasenburg.geobrorker.main.SimpleClient.java*.