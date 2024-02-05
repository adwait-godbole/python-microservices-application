import pika, json

def upload(file, gridfs, channel, access):
  try:
    file_id = gridfs.put(file)
  except Exception as err:
    print(err)
    return "internal server error", 500
  
  message = {
    "video_fid": str(file_id),
    "mp3_fid": None,
    "username": access["username"]
  }
  
  try:
    channel.basic_publish(
      exchange="",
      routing_key="video",
      body=json.dumps(message),  
      properties=pika.BasicProperties(
        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE # message should be persisted until it is deleted from the queue
      )
    )
  except Exception as err:
    print(err)
    gridfs.delete(file_id)
    return "internal server error", 500
