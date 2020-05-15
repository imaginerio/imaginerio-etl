# SCRIPT TO GENERANTE AN INTERACTIVE MAP USING BOKEH

import pandas as pd
import geopandas as gpd
import json

from bokeh.plotting import figure, output_file, show
from bokeh.tile_providers import get_provider, Vendors
from bokeh.models import GeoJSONDataSource, HoverTool, Circle, Patches, CustomJS, CustomJSFilter, TextInput, CDSView
from bokeh.layouts import column, layout
from pyproj import Proj, transform
from shapely import wkt

# Read file and transfom in geodataframe
df_file = pd.read_csv("./metadata/metadata.csv").head()
df_file['geometry'] = df_file['geometry'].apply(wkt.loads)
geo_df = gpd.GeoDataFrame(df_file, geometry = 'geometry', crs ='epsg:4326')

def LongLat_to_EN(x,y):
    try:
      e,n = transform(Proj('epsg:4326'), Proj('epsg:3857'), x,y)
      return e,n
    except:
      return None

geo_df['coordinates'] = geo_df.apply(lambda x: LongLat_to_EN(x['lat'],x['lng']),axis=1)

geo_df = geo_df.join(pd.DataFrame(geo_df['coordinates'].values.tolist(), columns=['e','n']))

geo_df['geometry'] = geo_df['geometry'].to_crs('epsg:3857')

geo_df = geo_df[['identifier','title', 'author', 'image','geometry','e','n']]

geosource = GeoJSONDataSource(geojson = geo_df.to_json())

#Description from points
TOOLTIPS = """
    <div style="margin: 5px; width: 300px;" >
      <img
          src="@image" alt="@image" height=200
          style="margin: 0px;"
          border="2"
          ></img>
        <h3 style='font-size: 10px; font-weight: bold;'>@identifier</h3>
        <p style='font-size: 10px; font-weight: light;'>@title</p>
        <p style='font-size: 10px; font-weight: light; font-style: italic;'>@author</p>
    </div>
"""

#Base map
maps = figure(title='Situated Views',
              x_axis_type='mercator',
              y_axis_type='mercator',
              plot_width=1400,
              plot_height=900)
tile_provider = get_provider(Vendors.CARTODBPOSITRON_RETINA)
maps.add_tile(tile_provider)

#construct points and wedges from hover

wedge = maps.patches(xs='xs',ys='ys', source=geosource, 
                    fill_color='white', fill_alpha=0, line_color= None,
                    hover_alpha=0.7, hover_fill_color='grey', hover_line_color='grey')

point = maps.circle(x='e',y='n', source=geosource,
                    size = 7,
                    fill_color='grey',
                    fill_alpha=0.5,
                    line_color='black')

h1 = HoverTool(renderers=[wedge], tooltips=None, mode='mouse')
h2 = HoverTool(renderers=[point], tooltips=TOOLTIPS, mode='mouse')
maps.add_tools(h1,h2)

output_file("situated-views-map.html")
show(maps)
