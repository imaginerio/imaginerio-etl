# Situated Views of Rio de Janeiro

> Repository for tracking technical issues and generating static files from kmls.

## Generating files for distribution

``` bash
# install dependencies, if you didn't already
$ npm install

# Generate files
$ npm run g

```

## Folder structure

```
situated-views
│   index.js
│   package.json
│   README.md
│   
└───dist-files (generated files for distribution)
│   │   situated-views.csv
│   │   situated-views.geojson   
│   
│     
└───kml-sessions (all kml files)
│   │   2019-04-12-Davi.kml
│   │   .
│   │   .
│   │   .
│   
└───sparql (sparql queries)
```


