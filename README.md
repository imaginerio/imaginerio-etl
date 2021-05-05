# Situated Views of Rio de Janeiro

## Commands



```bash
# set PWD variable (Windows users only):
$ $env:PWD=$PWD

# build an image named 'dagster-image' from Dockerfile at root
$ docker build -t dagster-image .

# run Dagit from a detached container named 'dagster-container' that listens to port 3000
$ docker run -p 3000:3000 -d --name dagster-container dagster-image -f modules/main.py
```
