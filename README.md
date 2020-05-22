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
$ npm run kml

# update images
$ python update_images.py

# update metadata
$ python update_data.py

```

## Folder structure

```
situated-views
│   index.html
│   package.json
│   Pipfile
│   environment.yml
│   README.md
│
└───images
│   └─  image samples for analysis
│
└───metadata
│   └─  data sources and reports
│
└───tools
    └─  scripts
```
