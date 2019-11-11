# Docker Readme

```bash
# build
docker build -t geobroker-server .

# run with default config, write logs to ./logs
docker run -p 5559:5559 -v {$PWD}/logs:/logs geobroker-server

# use custom config found at ./conf
docker run -p 5559:5559 -v {$PWD}/conf:/conf -v {$PWD}/logs:/logs geobroker-server /conf/config.toml
```

Helpful options:
- `-d` run in background/detached
