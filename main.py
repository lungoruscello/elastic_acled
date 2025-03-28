import pandas as pd

from elastic_acled import ACLEDIndexer


def main(pwd):
    rel_fpath = 'data/example_events.csv'  # change as needed
    acled = pd.read_csv(rel_fpath, low_memory=False)

    helper = ACLEDIndexer(
        index_name='test_acled',
        password=pwd,
        reset_index=True
    )
    helper.index_events(acled)


if __name__ == '__main__':
    es_pwd = "<your_elastic_pwd>"  # check README.md on where to find your password
    main(es_pwd)
