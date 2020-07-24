# Situated Views of Rio de Janeiro

## Commands

```bash
# up!
$ docker-compose up -d --build

# update images
$ docker-compose run app tools/update_images.py

# update metadata
$ docker-compose run app tools/update_data.py

# kml parser
# npm run kml (TO-DO: REWRITE KML PARSER IN PYTHON)

# down!
$ docker-compose down

```
