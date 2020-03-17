# GeoBroker

[![CodeFactor](https://www.codefactor.io/repository/github/moewex/geobroker/badge)](https://www.codefactor.io/repository/github/moewex/geobroker)

In the Internet of Things, the relevance of data often depends on the geographic context of data producers and consumers. Today’s data distribution services, however, mostly focus on data content and not on geo-context, which could help to reduce the dissemination of excess data in many IoT scenarios. We propose to use the geo-context information associated with devices to control data distribution.
For this, we designed GeoBroker, a data distribution service that uses the location of things, as well as geofences for messages and subscriptions, to control data distribution. This way, we enable new IoT application scenarios while also increasing overall system efficiency for scenarios where geo-contexts matter by delivering only relevant messages.

If you use this software in a publication, please cite it as:

### Text
Jonathan Hasenburg, David Bermbach. **GeoBroker: Leveraging Geo-Contexts for IoT Data Distribution**. In: Computer Communications. Elsevier 2020.

### BibTeX
```
@article{paper_hasenburg_geobroker,
	title = {{GeoBroker}: Leveraging Geo-Context for {IoT} Data Distribution},
	journal = {Elsevier Computer Communications},
	author = {Hasenburg, Jonathan and Bermbach, David},
	year = {2020}
}
```

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

