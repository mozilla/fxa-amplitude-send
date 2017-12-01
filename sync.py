import boto3
import os
import sys

from pyarrow import parquet

if __name__ == "__main__":
    argc = len(sys.argv)
    if argc == 2:
        parquet_file = parquet.ParquetFile(sys.argv[1])
        print("{} rows".format(parquet_file.metadata.num_rows))
        print(parquet_file.schema)

        # pyarrow.lib.ArrowNotImplementedError: lists with structs are not supported.
        #data = parquet_file.read()
        #print(data[0])

        # TODO: assert event_category == "sync" ?
        # PROBLEM: as per thom's email, server timestamp is in event_map_values, which pyarrow can't unpack...
        #          > pyarrow.lib.ArrowNotImplementedError: lists with structs are not supported.
        data = parquet_file.read(columns=["event_timestamp","event_method","event_object","event_string_value","uid","event_device_id","event_flow_id","app_name","app_version","event_device_os"])
        #print(data[9])
    else:
        sys.exit("Usage: {} <path-to-parquet-file>".format(sys.argv[0]))
