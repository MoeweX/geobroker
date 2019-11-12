# Docker Readme

Before building the Docker image, you have to run `mvn clean package` in the root directory.

Build with `docker build -t geobroker-client .`

## Usage

```bash
# use custom files found at ./files
docker run -v {$PWD}/files:/files -v {$PWD}/logs:/logs geobroker-client /files
```

Helpful options:
- `-d` run in background/detached
