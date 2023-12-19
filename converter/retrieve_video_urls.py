import requests
import re
import glob
import tensorflow as tf
from pymongo import MongoClient
import sys


def fetch_url(vid_id):
  # API for fetching url
  request_url = "http://data.yt8m.org/2/j/i/" + vid_id[:2] + "/" + vid_id + ".js"
  response = requests.get(request_url)

  # Check if the request was successful (status code 200)
  if response.status_code == 200:
    # response body is of the form: b'i("qiho","0JrRHaO5oCo");'
    try:
      response_string = response.content.decode('utf8')
      vid_url = re.findall('\"(.*?)\"', response_string.split(',')[1])[0]
      return vid_url
    except:
       print("Error: Invalid response")
  else:
     print(f"Failed to retrieve data. Status code: {response.status_code}")
  return 

def populate_urls(argv=None):
  if len(sys.argv) < 1:
      print("Error: Dataset path not specified ")
      return
  
  client = MongoClient('mongodb://localhost:27017')
  db = client['ytvids']
  coll = db['video-id-url-mapping']

  filenames = list(glob.glob(sys.argv[0]))
  for file in filenames:
    raw_dataset = tf.data.TFRecordDataset(tf.data.Dataset.from_tensor_slices([file]))
    for row in raw_dataset:
      example = tf.train.SequenceExample()
      tmp = example.FromString(row.numpy())
      context, video_features = tmp.context, tmp.feature_lists
      vid_id = context.feature['id'].bytes_list.value[0].decode('utf8')
      try:
        vid_url = fetch_url(vid_id)
        coll.insert_one({ vid_url : vid_id })
      except:
        print(vid_id, ": is problematic")

if __name__ == "__main__":
  populate_urls()

  