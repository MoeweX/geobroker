# Monitoring

Besides log files, GeoBroker-Server also includes a Prometheus support.
It is possible to enable prometheus by providing a port in the configuration,
when no port is given, or the port is not valid, prometheus support is disabled.

You can start a Prometheus server in a docker container by executing `./start.prom.sh`.
The server is available at **localhost:9090**.

Depending on your setup, you might have to update the scraping target, default is host.docker.internal:1234.