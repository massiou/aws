# Summary

s3_point_in_time.py aims at performing a S3 bucket snapshot given a timestamp

# Usage

```
s3_point_in_time.py [-h] -s BUCKET_SOURCE -d BUCKET_DEST -t TIMESTAMP -e ENDPOINT [-c CONNECTIONS]

optional arguments:
  -h, --help            show this help message and exit
  -s BUCKET_SOURCE, --bucket_source BUCKET_SOURCE
                        bucket source name
  -d BUCKET_DEST, --bucket_dest BUCKET_DEST
                        bucket destination name
  -t TIMESTAMP, --timestamp TIMESTAMP
                        timestamp limit
  -e ENDPOINT, --endpoint ENDPOINT
                        http endpoint
  -c CONNECTIONS, --connections CONNECTIONS
                        size of http connections pool
```


# Example

```
python3.6 s3_point_in_time.py -s "bucket-source" -d "bucket-snapshot" -t  1540987355 -e http://144.207.45.180 -c 128
```