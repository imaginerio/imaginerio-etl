# Situated Views of Rio de Janeiro


## Commands

```bash

# Build image
docker build -t container-name .

# Run command
docker run container-name {COMMAND}
docker run container-name dagster pipeline execute

# Update modified code without rebuilding
docker run -it --mount "type=bind,source=$(pwd)/src,target=/daster/src" {COMMAND}