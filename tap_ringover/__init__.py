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


def get_endpoint_field(endpoint, field):
    with open(get_abs_path('endpoints') + '/' + "endpoints.json") as file:
        data = json.load(file)[endpoint][field]
    return data


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

        key_properties = [get_endpoint_field(stream_id, "primary_key")]

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


def get_offset_query_param(data_length):
    return "&limit_offset=" + str(data_length)


def api_call(config, endpoint):

    data_length = 0
    http_status = 200
    data = []
    enlarge_limit_query_param = "?limit_count=1000"
    sub_object = get_endpoint_field(endpoint, "sub_object")
    continue_request = True

    while continue_request == True:
        headers = {'Content-Type': 'application/json',
                   'Authorization': config["api_key"]}

        response = requests.get(
            config["api_url_base"] + endpoint + enlarge_limit_query_param + get_offset_query_param(data_length), headers=headers)

        http_status = response.status_code

        LOGGER.info("API CODE : " + str(http_status))

        if http_status == 204:  # Empty endpoints
            break

        response_json = json.loads(response.content.decode('utf-8'))

        if type(response_json) is dict and 'limit_count_setted' in response_json.keys() and http_status is 200:
            continue_request = True
        else:
            continue_request = False

        data = data + response_json[sub_object] if sub_object else []
        data_length = len(data)

        time.sleep(0.6)  # Avoid 429 http status (too many requests)

    return list(filter(None, data))


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
            #singer.write_records(stream.tap_stream_id, [row])
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
