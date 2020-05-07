# Situated Views of Rio de Janeiro

> Repository for tracking technical issues and generating static files from kmls.

## Setup environment

```bash
# install dependencies
$ npm install
$ pipenv install
$ pipenv shell
```

## Commands

```bash
# kml parser
$ npm run g

# show map
bokeh serve --show tools/map.py

```

## Folder structure

```
situated-views
│   index.js
│   package.json
│   Pipfile.json
│   README.md
│
└───images
│   │   image samples for analysis
│
└───metadata
│   │   data sources and reports
│
└───tools
│   │   python scripts
```
