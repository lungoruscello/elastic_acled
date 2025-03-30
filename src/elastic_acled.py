"""
Functionalities to add geo-data on reported conflict events
from the ACLED project to a local Elasticsearch cluster.

Author: S. Langenbach (ETHZ)
Licence: MIT
"""
import warnings
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime

import pandas as pd
from elasticsearch import Elasticsearch, helpers
from tqdm.auto import tqdm
from urllib3.connectionpool import InsecureRequestWarning

ES_PORT = 9200

_SKELETON_CONFIG = dict(
        settings={
            'number_of_shards': 1,  # only one shard needed for single-node cluster
            'number_of_replicas': 1,
        },
        mappings=dict(
            properties={
                'document_type': {'type': 'keyword'}
            }
        )
    )


class ACLEDIndexer:
    document_type = 'acled_event'
    special_mappings = {
        'point_location': {'type': 'geo_point'}  # this ES field type supports geospatial queries
    }
    strftime = '%Y/%m/%d %H:%M:%S'

    def __init__(
            self,
            index_name,
            *,
            password,
            reset_index=False,
            **connect_kwargs
    ):
        self.index_name = index_name
        self.es_client = self.connect(password, **connect_kwargs)

        if not self.index_exists:
            self.create_index()
        else:
            if reset_index:
                self.delete_index()
                self.create_index()

    @property
    def index_exists(self):
        """
        Return True if the active ES connection already contains a
        namespace called `self.index_name`. Return False otherwise.
        """
        return bool(self.es_client.indices.exists(index=self.index_name))

    @property
    def config(self):
        """Return a `config` for the index."""
        config = deepcopy(_SKELETON_CONFIG)
        config['mappings']['properties'].update(**self.special_mappings)  # type: ignore
        return config

    def create_index(self):
        """
        Add `self.index_name` as a new namespace to the active ES
        connection.
        """
        if self.index_exists:
            raise RuntimeError(f"Index '{self.index_name}' already exists.")

        expected_return = {
            'acknowledged': True,
            'shards_acknowledged': True,
            'index': self.index_name
        }
        actual_return = self.es_client.indices.create(
            index=self.index_name, body=self.config
        )
        assert actual_return == expected_return

    def delete_index(self):
        """
        Delete namespace `self.index_name` from the active ES connection.
        Do nothing if the namespace `self.index_name` does not exist.
        """
        if self.index_exists:
            self.es_client.indices.delete(index=self.index_name)

    @staticmethod
    def connect(password, **kwargs):
        """
        Establish a connection with a local Elasticsearch service
        and return the connection object.

        Raises:
            RuntimeError if a connection cannot be established.
        """
        username = 'elastic'  # default username
        es_client = Elasticsearch(
            f'https://localhost:{ES_PORT}',
            basic_auth=(username, password),  # Use basic authentication
            request_timeout=60,
            max_retries=2,
            verify_certs=False,  # TODO: Add certificate verification
            **kwargs
        )
        if not es_client.ping():
            raise RuntimeError('Could not connect with Elasticsearch.')

        print(f'Connected to Elasticsearch on port {ES_PORT}!')
        return es_client

    def index_events(
            self,
            event_df: pd.DataFrame,
            id_column: str = 'event_id_cnty',  # ACLED event identifier
            chunk_size: int = 500
    ):
        event_df['event_date'] = pd.to_datetime(event_df['event_date'])

        actions = self._stream_actions(  # returns a generator
            event_df,
            id_col=id_column,
            pbar_desc='Indexing event records'
        )

        with silence_warning(InsecureRequestWarning):
            successes, failures = helpers.bulk(
                client=self.es_client,
                actions=actions,
                chunk_size=chunk_size,
                stats_only=True
            )
            self.es_client.indices.refresh(index=self.index_name)

        print(f"Successfully indexed {successes} records.")
        if failures:
            print(f"{failures} records failed to index.")

    def _make_es_document(self, record: pd.Series) -> dict:
        # Make the 'event_date' field JSON-serialisable
        record['event_date'] = record['event_date'].strftime(self.strftime)

        # Create a 'point_location' field, using the proper data format
        # expected by the 'geo_point' type to which this field is mapped
        record['point_location'] = dict(
            lat=record['latitude'],
            lon=record['longitude']
        )

        # Drop redundant fields
        for drop_key in 'longitude latitude geometry timestamp region year'.split():
            if drop_key in record.keys():
                record.pop(drop_key)

        record['modified_time'] = datetime.now().strftime(self.strftime)
        record['document_type'] = self.document_type

        record = self._normalise_empty_fields(record)

        return record.to_dict()

    @staticmethod
    def _normalise_empty_fields(record):
        """
        Set missing values and empty strings within all fields of a
        given `record` to 'None'.
        """
        not_null = pd.notna(record)  # captures NaN, NaT and 'None' (but not empty lists)
        record = record.where(not_null, other=None)

        for key, value in record.items():
            if isinstance(value, str) and value == '':
                record[key] = None
        return record

    def _stream_actions(
            self,
            df: pd.DataFrame,
            id_col: str,
            pbar_desc='Indexing'
    ):
        """
        Convert each row in a dataframe to an Elasticsearch 'document'. Then
        yield each document as an indexing action that can be consumed by
        the Elasticsearch `bulk` facility.
        """
        row_iter = tqdm(
            df.iterrows(), total=len(df), desc=pbar_desc, leave=False
        )
        num_skipped = 0
        for _, record in row_iter:
            doc = self._make_es_document(record)
            if doc:
                action_dict = {
                    '_index': self.index_name,
                    '_id': doc[id_col],
                    '_source': doc
                }
                yield action_dict
            else:
                num_skipped += 1

        if num_skipped:
            print(f'{num_skipped} records skipped due to unknown exception.')


@contextmanager
def silence_warning(warning_type):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", warning_type)
        yield
