

curl http://andrey.blednykh2:AkRjOhAK@10.0.0.5:9200/

# reate index

curl -X PUT -H "Content-Type: application/json" 'http://andrey.blednykh2:AkRjOhAK@10.0.0.5:9200/andrey_blednykh2_lab08?include_type_name=false' -d @-<<END
{
   "settings" : {
        "number_of_shards" : 1,
        "number_of_replicas" : 1
    },

   "mappings": {
    "properties": {
      "uid": {
        "type": "keyword",
        "null_value": "NULL"
      },
      "gender_age": {
        "type": "keyword"
      },
      "date": {
        "type": "date"
      }
    }
  }
}
END