import dlt

@dlt.table

def dimartist_stg():
    df=spark.readStream.table('spotify_project.schema.dimartist')
    return df

dlt.create_streaming_table("dimartist_scd2")
 
dlt.create_auto_cdc_flow(
  target = "dimartist_scd2",
  source = "dimartist_stg",
  keys = ["artist_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  track_history_except_column_list=None,
  name=None,
  once=False
)