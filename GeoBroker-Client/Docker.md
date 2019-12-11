# Docker Readme

Before building the Docker image, you have to run `mvn clean package` in the root directory.

Build with `docker build -t geobroker-client .`

## Usage

```bash
# To see a list of options
docker run geobroker-client -h

# you have to supply wich files to use (here via volume mounted at /files)
docker run -v {$PWD}/files:/files -v {$PWD}/logs:/logs geobroker-client -d /files
```

Helpful options:
- `-d` run in background/detached
