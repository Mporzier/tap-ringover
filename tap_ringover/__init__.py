#!/usr/bin/env python3

# Import os for files and directories discovery
import os
# Import json functions.
import json
# Import time for sleep function.
import time
# Import requests for HTTP calls.
import requests
# Import singer miscellaneous functions.
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

# Required keys in config file passed as an argument.
REQUIRED_CONFIG_KEYS = ["api_url_base", "api_key"]

# Initializes the logger for terminal output.
LOGGER = singer.get_logger()


# Get the absolute path of a file or folder's relative path [path].
def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Get a data [field] related to a Ringover endpoint [endpoint].
def get_endpoint_field(endpoint, field):
    with open(get_abs_path('endpoints') + '/' + "endpoints.json") as file:
        data = json.load(file)[endpoint][field]
    return data


# Load all schemas from the "schemas" folder into an object and returns it.
def load_schemas():
    # Schemas object.
    schemas = {}
    # For each schema in the schemas folder.
    for filename in os.listdir(get_abs_path('schemas')):
        # Get the absolute path of the current schema.
        path = get_abs_path('schemas') + '/' + filename
        # Get the name of the schema.
        file_raw = filename.replace('.json', '')
        # Load the current schema from the schema json file into the schemas object.
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


# Get the catalog object containing all the schemas and streams data and returns it.
def discover():
    # Load the schemas object.
    raw_schemas = load_schemas()
    # Streams object.
    streams = []
    # For each schema.
    for stream_id, schema in raw_schemas.items():
        # Metadatas (empty here as no one is used).
        stream_metadata = []
        # Schemas' primary keys (main SQL key).
        key_properties = [get_endpoint_field(stream_id, "primary_key")]
        # Populate the current stream's datas.
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


# Get the offset string to add to the query string from the length of the already retrieved data.
def get_offset_query_param(data_length):
    return "&limit_offset=" + str(data_length)


# Execute API calls.
def api_call(config, endpoint):
    # Ringover API's blacklist endpoint is badly done and we need to add a suffix to its URL.
    blacklist = "/numbers" if endpoint == "blacklists" else ""
    # Length of the retrieved data.
    data_length = 0
    # Status of the last HTTP call.
    http_status = 200
    # Retrieved data.
    data = []
    # Query string suffix to enlarge the amount of data retrieved for each call.
    enlarge_limit_query_param = "?limit_count=1000"
    # Get the name of the sub object that contains the data we want (different for each endpoint).
    sub_object = get_endpoint_field(endpoint, "sub_object")
    # Flag to stop the request.
    continue_request = True

    # While the request is incomplete.
    while continue_request == True:
        headers = {'Content-Type': 'application/json',
                   'Authorization': config["api_key"]}
        response = requests.get(
            config["api_url_base"] + endpoint + blacklist + enlarge_limit_query_param + get_offset_query_param(data_length), headers=headers)
        http_status = response.status_code
        # Empty endpoints
        if http_status == 204:
            break
        response_json = json.loads(response.content.decode('utf-8'))
        if type(response_json) is dict and 'limit_count_setted' in response_json.keys() and http_status is 200:
            continue_request = True
        else:
            continue_request = False
        data = data + \
            response_json[sub_object] if sub_object else response_json
        data_length = len(data)
        # Avoid 429 http status (too many requests)
        time.sleep(0.5)
        # Calls mysteriously does not have a limit_offset support on the Ringover's API, so we have to break.
        if endpoint == "calls":
            break
    # Return a list containing all the retrieved data.
    return list(filter(None, data))


# Sync data from tap source.
def sync(args, catalog):
    # Loop over selected streams in catalog.
    for stream in catalog.streams:
        bookmark_column = stream.replication_key
        # Indicates if the data is already sorted from the HTTP call.
        is_sorted = True
        # Outputs the schema with the correct singer format.
        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )
        # Get the API's data.
        data = api_call(args.config, stream.tap_stream_id)
        # Boomark for unsorted data.
        max_bookmark = None
        # For each data's row.
        for row in data:
            # Write one or more rows to the stream.
            singer.write_records(stream.tap_stream_id, [row])
    return


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump the output.
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode.
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args, catalog)


if __name__ == "__main__":
    main()
