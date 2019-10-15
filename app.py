from flask import Flask, render_template, Response, g
from models import Database
import geopandas as gpd
import datetime as dt

import json

from bokeh.embed import components
from bokeh.plotting import figure
from bokeh.resources import INLINE
from bokeh.util.string import encode_utf8
from bokeh.models import GeoJSONDataSource, LinearColorMapper, ColorBar, HoverTool, ColumnDataSource, Range1d, \
    LinearAxis, Grid, Label
from bokeh.models.glyphs import ImageURL
from bokeh.layouts import column
from bokeh.palettes import brewer

app = Flask(__name__)

if app.config["ENV"] == "LocalProd":
    app.config.from_object("config.LocalProdConfig")
elif app.config["ENV"] == "LocalTest":
    app.config.from_object("config.LocalTestConfig")
else:
    app.config.from_object("config.AWSProdConfig")

db = Database()

N_DATAPOINTS = 20
DEFAULT_VARIABLE = 'All'


@app.route('/')
def index():
    df, last_update = db.get_grid_data_db()
    df['jpeg'] = 'static/images/' + df['jpeg']
    max_sent = abs(df['avg_sent']).max()
    df['avg_sent'] = df['avg_sent'] / max_sent

    cands = db.get_candidates()

    xdr = Range1d(start=-0, end=df['user_count'].max() + 1000)
    ydr = Range1d(start=-1.2, end=1.2)

    source = ColumnDataSource(df)
    p = figure(plot_height=600, plot_width=800, x_range=xdr, y_range=ydr, toolbar_location=None)
    #p = figure(sizing_mode="scale_both", x_range=xdr, y_range=ydr, toolbar_location=None)
    p.circle(x="user_count", y="avg_sent", size=50, source=source, alpha=.05)
    image = ImageURL(url="jpeg", x="user_count", y="avg_sent", w=None, h=None, anchor="center", global_alpha=.75)
    p.add_glyph(source, image)
    p.add_tools(HoverTool(
        tooltips=[
            ("Candidate", "@fullname"),
            ("Sentiment", "@avg_sent"),
            ("#Tweeters", "@user_count"),
        ]))
    p.toolbar.active_drag = None
    p.sizing_mode = 'scale_width'

    xaxis = LinearAxis()
    p.xaxis.fixed_location = 0
    p.xaxis.axis_line_width = 2
    p.xaxis.axis_label = 'Number of Tweeters'
    p.xaxis.axis_label_text_font_style = 'bold'
    p.xaxis.axis_label_text_font_size = '12pt'
    p.xaxis.major_label_text_font_style = 'bold'
    p.xaxis.major_label_text_font_size = '12pt'

    label = Label(x=df['user_count'].max()/2, y=-.2, text='Number of Tweeters', level='glyph', render_mode='canvas',
                  text_font_style='bold', text_font_size = '12pt')

    yaxis = LinearAxis()
    p.yaxis.axis_line_width = 2
    p.yaxis.axis_label = 'Sentiment'
    p.yaxis.axis_label_text_font_style = 'bold'
    p.yaxis.axis_label_text_font_size = '12pt'
    p.yaxis.major_label_text_font_style = 'bold'
    p.yaxis.major_label_text_font_size = '12pt'

    p.add_layout(Grid(dimension=0, ticker=xaxis.ticker))
    p.add_layout(Grid(dimension=1, ticker=yaxis.ticker))
    p.add_layout(label)

    js_resources = INLINE.render_js()
    css_resources = INLINE.render_css()
    layout = column(p)
    script, div = components(layout)
    html = render_template(
        'main.html',
        plot_script=script,
        plot_div=div,
        js_resources=js_resources,
        css_resources=css_resources,
        last_update=last_update,
        cands=cands
    )
    return encode_utf8(html)


@app.route('/map/<candidate>', methods=['GET', 'POST'])
def map(candidate):
    if candidate == '':
        candidate = 'All'
    else:
        candidate = candidate
    cands = db.get_candidates()
    max_sent, min_sent = db.get_scaled_sent()

    shapefile = app.config["PATH"]
    gdf = gpd.read_file(shapefile)
    df_states = db.get_states()
    gdf = gdf.merge(df_states, left_on='NAME', right_on='state')
    gdf_us = gdf[['abbr', 'state', 'geometry']].copy()
    gdf_us.columns = ['abbr', 'name', 'geometry']
    gdf_us = gdf_us[~gdf_us['abbr'].isin(['AK', 'AS', 'GU', 'HI', 'MP', 'PR', 'VI'])]

    last_update, tweets, df = db.get_candidate_data(candidate, 0)
    df['sent'] = round(df['sent'] * 1, 2)
    merged = gdf_us.merge(df, how='left', left_on='abbr', right_on='state')
    merged = merged[merged['user_count'] > 0].copy()

    merged_json = json.loads(merged.to_json())
    json_data = json.dumps(merged_json)

    geosource = GeoJSONDataSource(geojson=json_data)
    palette = brewer['RdBu'][8]
    palette = palette[::-1]
    bar_range = max(abs(max_sent), abs(min_sent))
    color_mapper = LinearColorMapper(palette=palette, low=-bar_range, high=bar_range)

    color_bar = ColorBar(color_mapper=color_mapper, label_standoff=8, width=20,
                         border_line_color='black', location=(0, 0))
    p = figure(title=None, plot_height=550, plot_width=1000, toolbar_location=None)
    p.xgrid.grid_line_color = None
    p.ygrid.grid_line_color = None
    p.add_tools(HoverTool(
        tooltips=[
            ("State", "@name"),
            ("#Tweeters", "@user_count"),
            ("Sentiment", "@sent"),
        ]))
    p.axis.visible = False
    p.add_layout(color_bar, 'right')

    p.patches('xs', 'ys', source=geosource, fill_color={'field': 'sent', 'transform': color_mapper},
              line_color='black', line_width=0.25, fill_alpha=.75)

    js_resources = INLINE.render_js()
    css_resources = INLINE.render_css()

    layout = column(p)
    script, div = components(layout)
    html = render_template(
        'map.html',
        plot_script=script,
        plot_div=div,
        js_resources=js_resources,
        css_resources=css_resources,
        last_update=last_update,
        tweets=tweets,
        selected_cand=candidate,
        cands=cands
    )
    return encode_utf8(html)


@app.route('/grid', methods=['GET', 'POST'])
def grid():
    df, last_update = db.get_grid_data_db()
    df['jpeg'] = 'static/images/' + df['jpeg']
    max_sent = abs(df['avg_sent']).max()
    df['avg_sent'] = df['avg_sent'] / max_sent

    xdr = Range1d(start=-0, end=df['user_count'].max() + 1000)
    ydr = Range1d(start=-1.2, end=1.2)

    source = ColumnDataSource(df)
    p = figure(plot_height=600, plot_width=800, x_range=xdr, y_range=ydr, toolbar_location=None)
    p.circle(x="user_count", y="avg_sent", size=50, source=source, alpha=.05)
    image = ImageURL(url="jpeg", x="user_count", y="avg_sent", w=None, h=None, anchor="center", global_alpha=.75)
    p.add_glyph(source, image)
    p.add_tools(HoverTool(
        tooltips=[
            ("Candidate", "@fullname"),
            ("Sentiment", "@avg_sent"),
            ("#Tweeters", "@user_count"),
        ]))

    xaxis = LinearAxis()
    p.xaxis.fixed_location = 0
    p.xaxis.axis_line_width = 2
    p.xaxis.axis_label = 'Number of Tweeters'
    p.xaxis.axis_label_text_font_style = 'bold'
    p.xaxis.axis_label_text_font_size = '12pt'
    p.xaxis.major_label_text_font_style = 'bold'
    p.xaxis.major_label_text_font_size = '12pt'

    yaxis = LinearAxis()
    p.yaxis.axis_line_width = 2
    p.yaxis.axis_label = 'Sentiment'
    p.yaxis.axis_label_text_font_style = 'bold'
    p.yaxis.axis_label_text_font_size = '12pt'
    p.yaxis.major_label_text_font_style = 'bold'
    p.yaxis.major_label_text_font_size = '12pt'

    p.add_layout(Grid(dimension=0, ticker=xaxis.ticker))
    p.add_layout(Grid(dimension=1, ticker=yaxis.ticker))

    js_resources = INLINE.render_js()
    css_resources = INLINE.render_css()
    layout = column(p)
    script, div = components(layout)
    html = render_template(
        'grid.html',
        plot_script=script,
        plot_div=div,
        js_resources=js_resources,
        css_resources=css_resources,
        last_update=last_update,
    )
    return encode_utf8(html)


@app.route('/about')
def about():
    return render_template('about.html')


if __name__ == '__main__':
    app.run()
