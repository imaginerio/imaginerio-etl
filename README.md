# Situated Views of Rio de Janeiro

## Commands



```bash
# set PWD variable (Windows users only):
$ $env:PWD=$PWD

# up!
$ docker-compose up -d --build

# update images
$ docker-compose run app modules/update_images.py

# update metadata
$ docker-compose run app modules/update_data.py

# kml parser
# npm run kml (TO-DO: REWRITE KML PARSER IN PYTHON)

# down!
$ docker-compose down

```
