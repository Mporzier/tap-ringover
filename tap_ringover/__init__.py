#!/usr/bin/env python3
import os
import json
import jsons
import singer
import time
import requests
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

REQUIRED_CONFIG_KEYS = ["api_url_base", "api_key"]

LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []

        ###

        path = get_abs_path('endpoints') + '/' + "endpoints.json"
        with open(path) as file:
            sub_object = json.load(file)[stream_id]["primary_key"]

        key_properties = [sub_object]

        ###

        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


def api_call(config, endpoint):
    headers = {'Content-Type': 'application/json',
               'Authorization': config["api_key"]}

    response = requests.get(config["api_url_base"] + endpoint, headers=headers)
    time.sleep(0.2)
    data = json.loads(response.content.decode('utf-8')
                      ) if response.status_code != 204 else {}

    path = get_abs_path('endpoints') + '/' + "endpoints.json"
    with open(path) as file:
        sub_object = json.load(file)[endpoint]["sub_object"]

    return data[sub_object] if sub_object and data else data


def sync(args, catalog):
    """ Sync data from tap source """

    # Loop over selected streams in catalog
    for stream in catalog.streams:
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        data = api_call(args.config, stream.tap_stream_id)

        max_bookmark = None
        for row in data:
            # TODO: place type conversions or transformations here

            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, [row])
            if bookmark_column:
                if is_sorted:
                    # update bookmark to latest value
                    singer.write_state(
                        {stream.tap_stream_id: row[bookmark_column]})
                else:
                    # if data unsorted, save max value until end of writes
                    max_bookmark = max(max_bookmark, row[bookmark_column])
        if bookmark_column and not is_sorted:
            singer.write_state({stream.tap_stream_id: max_bookmark})
    return


@utils.handle_top_exception(LOGGER)
def main():
    """
    catalog = discover()
    singer.write_schema('contacts', jsons.dump(catalog), ['contact_id'])

    api_token = '921d8d7cb9bc2f770f5c4eceb6b223223c243473'
    api_url = 'https://public-api.ringover.com/v2/contacts'

    headers = {'Content-Type': 'application/json',
               'Authorization': api_token}

    with requests.get(api_url, headers=headers) as response:
        data = json.loads(response.content.decode('utf-8'))
        singer.write_records('contacts', data.get('contact_list'))
    """

    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args, catalog)


if __name__ == "__main__":
    main()
