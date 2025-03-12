from pathlib import Path

import seaborn as sns
from matplotlib import pyplot as plt
from pyproj import CRS

__all__ = [
    "ROOT_DIR",
    "ES_PORT",
    "LATLON",
    "ALBERS_AFRICA",
    "RED",
    "format_geo_axis"
]

ROOT_DIR = Path(__file__).parent.parent
ES_PORT = 9200

LATLON = CRS("EPSG:4326")  # GPS standard (epsg.io/4326). Datum: WGS84, unit: degrees.
ALBERS_AFRICA = CRS("ESRI:102022")  # Africa Albers Equal Area Conic (epsg.io/102022). Datum: WGS84, unit: metres.
RED = 'xkcd:brick red'


def format_geo_axis(
        ax=None,
        title='',
        fully_despine=True,
):
    ax = plt.gca() if ax is None else ax
    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.set_xticks([])
    ax.set_yticks([])
    equal_axis_aspect(ax)

    if fully_despine:
        sns.despine(left=True, bottom=True)

    ax.set_title(title)


def equal_axis_aspect(ax=None):
    if ax is None:
        ax = plt.gca()
    ax.set_aspect("equal", "box")
