# Situated Views of Rio de Janeiro

## Commands



```bash
# set PWD variable (Windows users only):
$ $env:PWD=$PWD

# up!
$ docker-compose up -d --build

# update images
$ docker-compose run app modules/update_images.py

# update metadata (requires moving tiles and index.html after running)
$ docker-compose run app modules/update_data.py

# kml parser
# npm run kml (TO-DO: CHANGE TO CONES.PY FROM CONES BRANCH)

# down!
$ docker-compose down

```
