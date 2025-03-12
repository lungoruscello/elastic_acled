import geopandas as gpd

from elastic_acled import ACLEDIndexer
from misc import LATLON


def main(pwd):
    rel_fpath = 'data/acled_master_africa.feather'
    acled = gpd.read_feather(rel_fpath)
    assert acled.crs == LATLON  # check the Coordinate Reference System (lat/lon, WGS84)

    helper = ACLEDIndexer(
        index_name='acled_africa',
        password=pwd,
        reset_index=True
    )
    helper.index_events(acled)


if __name__ == '__main__':
    es_pwd = "<your_elastic_pwd>"
    main(es_pwd)
