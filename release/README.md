# GeoBroker

This folder contains the GeoBroker binaries (*GeoBroker-Server.jar*), as well as a client implementation (*MultiFileClient.jar*) that can be used for a small scale experiment.
Before you continue, please unzip the provided example data (*MultiFileClient_data.zip*).

## Starting the Server

The GeoBroker-Server needs a configuration file at startup (*GeoBroker_config.toml*).
To start GeoBroker-Server, run:
```bash
java -jar GeoBroker-Server.jar GeoBroker_config.toml
```

Logging levels depend on the log4j2 configuration (*GeoBroker_log4j2.xml*).

## Using the MultiFileClient

The MultiFileClient sends messages on behalf of multiple clients to the configured GeoBroker server.
What messages will be send is defined through files that each comprise instructions for a single client.

To start the MultiFileClient and connect to the GeoBroker-Server (running on localhost), run:
```bash
java -jar MultiFileClient.jar -d MultiFileClient_data/ -l MultiFileClient_log4j2.xml
```

When the MultiFileClient distributed all client messages (takes about 5 minutes), it writes what messages have been sent and received at what time to *MultiFileClient_data/wasSent.txt* and *MultiFileClient_data/wasReceived.txt*

To see what other configuration options for the MultiFileClient are available, run `java -jar MultiFileClient --help`. Again, logging levels depend on the log4j2 configuration (*MultiFileClient_log4j2.xml*).

## Effects of using Geo-Context for the Matching of Messages

To better understand the effects of using geo-context for the matching of messages, update `[server.mode]` in *GeoBroker_config.toml* from `name = "single"` to `name = "single_noGeo"`.
Afterwards, start the GeoBroker-Server and MultiFileClient again; while the wasSent.txt file will have the same size, the wasReceived.txt file is substantially larger. The reason for this is that clients now also receive messages that are not relevant for them as messages are not filtered by GeoBroker-Server based on the geo-context information provided by clients.
