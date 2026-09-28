import dlt

@dlt.table

def factstream_stg():
    df=spark.readStream.table('spotify_project.schema.factstream')
    return df

dlt.create_streaming_table("factstream_scd2")
 
dlt.create_auto_cdc_flow(
  target = "factstream_scd2",
  source = "factstream_stg",
  keys = ["stream_id"],
  sequence_by = "stream_timestamp",
  stored_as_scd_type = 1,
  track_history_except_column_list=None,
  name=None,
  once=False
)