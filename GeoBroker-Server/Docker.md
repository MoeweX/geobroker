# Docker Readme

Before building the Docker image, you have to run `mvn clean package` in the root directory.

Build with `docker build -t geobroker-server .`

## Usage

```bash
# run with default config, write logs to ./logs
docker run -p 5559:5559 -v {$PWD}/logs:/logs geobroker-server

# use custom config found at ./conf
docker run -p 5559:5559 -v {$PWD}/conf:/conf -v {$PWD}/logs:/logs geobroker-server /conf/config.toml
```

Helpful options:
- `-d` run in background/detached
