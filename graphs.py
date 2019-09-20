import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
plt.ioff()
import matplotlib.dates as mdates
from models import Database
from matplotlib.figure import Figure
from datetime import datetime


def get_graph_grid(df, last_update):
    x = df['user_count'].values
    y = df['avg_sent'].values

    fig, ax = plt.subplots()
    ax.scatter(x, y)

    for i, txt in enumerate(df['candidate'].values):
        jpg_loc = df[df['candidate'] == txt]['jpeg'].values[0]
        filename = 'static/images/' + jpg_loc
        image = plt.imread(filename)
        im = OffsetImage(image, zoom=.5)
        ab = AnnotationBbox(im, (x[i], y[i]), xycoords='data', frameon=False)
        artists = []
        artists.append(ax.add_artist(ab))
        ax.annotate(txt, (x[i], (y[i])))

    ax.set_title('Democratic Hopefuls Sentiment \n Last update: %s' % last_update)

    # Move left y-axis and bottim x-axis to centre, passing through (0,0)
    #ax.spines['left'].set_position('center')
    ax.spines['bottom'].set_position('center')

    # Eliminate upper and right axes
    ax.spines['right'].set_color('none')
    ax.spines['top'].set_color('none')

    ax.set_ylim([-.05, .05])

    # Show ticks in the left and lower axes only
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')
    fig.savefig('static/graph.png')
    return fig

