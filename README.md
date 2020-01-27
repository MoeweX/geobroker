# GeoBroker

[![CodeFactor](https://www.codefactor.io/repository/github/moewex/geobroker/badge)](https://www.codefactor.io/repository/github/moewex/geobroker)

GeoBroker is a pub/sub system research prototype that not only uses content information, i.e., the topic of a message, 
but also the geo-context of publishers and subscribers for the matching of messages.

Related Publication:  
Jonathan Hasenburg, David Bermbach. **GeoBroker: Leveraging Geo-Contexts for IoT Data Distribution**. In: Computer Communications, 151, 473-484. Elsevier 2020.  [Bib](https://www.mcc.tu-berlin.de/fileadmin/fg344/publications/2020-06-01_geobroker.bib). [PDF](https://arxiv.org/pdf/2001.01603.pdf).

## Quickstart

As this project contains multiple git submodules, one needs to run the following after cloning:
```
git submodule init
git submodule update
```

To start the Server, run the main method of *de.hasenburg.geobroker.server.main.Server.java*. 
A very simple client can be started by running *de.hasenburg.geobroker.client.main.SimpleClient.java*.

## Usage of Code in Publications

The code found in this repository has been used for the experiments of the GeoBroker paper.
- The original code for GEO experiments (geo-context information is used) is accessible in the GEO branch or release 
[v1.0](https://github.com/MoeweX/geobroker/releases/tag/v1.0).
- The original code for NoGEO experiments (geo-context information is not used) is accessible in the NoGEO branch or 
release [v1.0-noContext](https://github.com/MoeweX/geobroker/releases/tag/v1.0-noContext).

