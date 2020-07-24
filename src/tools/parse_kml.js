const fs = require("fs");
const xml2js = require("xml2js");
let path = require("path");
let pathDir = path.resolve("./") + "/metadata/camera/kml-sessions";
let distDir = "metadata/camera/";
const distfileName = "camera";
const turf = require("@turf/turf");

const parser = new xml2js.Parser();
const builder = new xml2js.Builder();

const calculateFOVdistance = (height, tilt) => {
  const minDistanceValue = 400;
  const distanceExtrapolation = 10;
  // tan = opposite leg / adjacent leg :. opposite leg = adjacent leg * tan
  if (tilt <= 89) {
    const tan = Math.tan((tilt * Math.PI) / 180);
    const distance = height * tan;
    if (distance < minDistanceValue) return minDistanceValue;
    return distance + distanceExtrapolation * ((height * 2) / 3);
  } else {
    return minDistanceValue;
  }
};

const removeStringExtension = (str) => {
  if (str.lastIndexOf(".") === -1) return str;
  return str.substring(0, str.lastIndexOf("."));
};

let kml = [];
const kmlTokml = (photoOverlays) => {
  return;
  fs.writeFileSync(
    `${distDir}/${distfileName}.json`,
    JSON.stringify(photoOverlays)
  );
};

let features = [];
const kmlToGeojson = (photoOverlays) => {
  photoOverlays.forEach((photo) => {
    const feature = {
      type: "Feature",
      properties: {
        name: photo.name[0],
        angle: 50,
        height: parseFloat(photo.Camera[0].altitude[0]),
        heading: parseFloat(photo.Camera[0].heading[0]),
        bearing: parseFloat(photo.Camera[0].heading[0]),
        distance: calculateFOVdistance(
          parseFloat(photo.Camera[0].altitude[0]),
          parseFloat(photo.Camera[0].tilt[0])
        ),
        tilt: parseFloat(photo.Camera[0].tilt[0]),
      },
      geometry: {
        type: "Point",
        coordinates: [
          parseFloat(photo.Camera[0].longitude[0]),
          parseFloat(photo.Camera[0].latitude[0]),
        ],
      },
    };

    const point = feature.geometry;
    const bearing1 =
      feature.properties.bearing + parseFloat(photo.ViewVolume[0].leftFov[0]);
    const bearing2 =
      feature.properties.bearing + parseFloat(photo.ViewVolume[0].rightFov[0]);
    const distance = feature.properties.distance;
    const sorvete = turf.sector(point, distance / 1000, bearing1, bearing2);

    //need to recreate the feature because convex() overrides it properties
    features.push({ ...feature, geometry: sorvete.geometry });
  });

  const file = {
    type: "FeatureCollection",
    features: features,
  };
  fs.writeFileSync(`${distDir}/${distfileName}.geojson`, JSON.stringify(file));
};

let lines = "";
const kmlToCSV = (photoOverlays) => {
  photoOverlays.forEach((photo) => {
    const name = removeStringExtension(photo.name[0]);
    const fov = parseFloat(photo.ViewVolume[0].rightFov[0]).toFixed(1) * 2;
    const camera = photo.Camera[0];
    const lat = parseFloat(camera.latitude[0]).toFixed(6);
    const long = parseFloat(camera.longitude[0]).toFixed(6);
    const height = parseFloat(camera.altitude[0]).toFixed(2);
    const heading = parseFloat(camera.heading[0]).toFixed(2);
    const tilt = parseFloat(camera.tilt[0]).toFixed(2);

    const line = `${name},${lat},${long},${height},${heading},${tilt},${fov}\n`;
    lines = lines + `${line}`;
  });

  const file = `name,lat,long,height,heading,tilt,fov\n${lines}`;
  fs.writeFileSync(`${distDir}/${distfileName}.csv`, file);
};

let linesWikdata = "";
const kmlToCSVWikidata = (photoOverlays) => {
  photoOverlays.forEach((photo) => {
    const name = removeStringExtension(photo.name[0]);
    const camera = photo.Camera[0];
    const lat = parseFloat(camera.latitude[0]).toFixed(6);
    const long = parseFloat(camera.longitude[0]).toFixed(6);

    const line = `${name},@${lat}/${long}\n`;
    linesWikdata = linesWikdata + `${line}`;
  });

  const file = `name,@lat/long\n${linesWikdata}`;
  fs.writeFileSync(`${distDir}/${distfileName}-wikidata.csv`, file);
};

// loop through all files in 'path'
let extractor = (error, files) => {
  if (error) {
    console.error("Could not list the directory.", error);
    process.exit(1);
  }

  files.forEach(function (file, index) {
    let fromPath = path.join(pathDir, file);
    fs.stat(fromPath, function (error2, stat) {
      if (error2) {
        console.error("Error stating file.", error2);
        return;
      }
      if (stat.isFile()) {
        fs.readFile(fromPath, function (err, data) {
          parser.parseString(data, function (err, result) {
            if (err) {
              console.error("Error parsing kml file.", err);
              return;
            }
            const photoOverlays = result.kml.Folder[0].PhotoOverlay;

            kmlToCSV(photoOverlays);
            kmlToGeojson(photoOverlays);
            kmlTokml(photoOverlays);
            // kmlToCSVWikidata(photoOverlays);
            console.log(file, "done.");
          });
        });
      }
    });
  });
};

//run the bootstrap
fs.readdir(pathDir, extractor);
