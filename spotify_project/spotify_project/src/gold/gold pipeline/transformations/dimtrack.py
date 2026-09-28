import dlt

@dlt.table

def dimtrack_stg():
    df=spark.readStream.table('spotify_project.schema.dimtrack')
    return df

dlt.create_streaming_table("dimtrack_scd2")
 
dlt.create_auto_cdc_flow(
  target = "dimtrack_scd2",
  source = "dimtrack_stg",
  keys = ["track_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  track_history_except_column_list=None,
  name=None,
  once=False
)