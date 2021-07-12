import geojson

camera = "data/output/camera.geojson"
with open(camera) as f:
    gj = geojson.load(f)
print((gj[0]))
