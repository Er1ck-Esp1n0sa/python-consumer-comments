# Import some necessary modules
# pip install kafka-python
# pip install pymongo
# pip install "pymongo[srv]"
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi

import json

uri = "mongodb+srv://erick:1234@python.aj67na4.mongodb.net/?retryWrites=true&w=majority"


# Create a new client and connect to the server
#client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection

#try:
#    client.admin.command('ping')
#    print("Pinged your deployment. You successfully connected to MongoDB!")
#except Exception as e:
#    print(e)

# Connect to MongoDB and pizza_data database


try:
    client = MongoClient(uri)
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")

    db = client.python
    print("MongoDB Connected successfully!")
except:
    print("Could not connect to MongoDB")

#Para consumir el topic comments
consumerComments = KafkaConsumer('comments',bootstrap_servers=['my-kafka-0.my-kafka-headless.er1ck-esp1n0sa.svc.cluster.local:9092']) 
for msg in consumerComments:
    record = json.loads(msg.value)
    print(record)

    info = {'name':record['name'],
    'publication':record['publication'],
    'comment':record['comment']}

    try:
        info_id = db.comments_info.insert_one(info)
        print("Data inserted with record ids", info_id)
    except:
        print("Could not insert into MongoDB")

    try:
        agg_result = db.comments_info.aggregate(
            [{
                "$group" : { "_id" : "$publication",
                             "total":{"$sum":1}
                            }
            }]
        )
        db.comments_summary.delete_many({})
        for i in agg_result:
            print(i)
            summary_id = db.comments_summary.insert_one(i)
            print("Summary inserted with record ids", summary_id)
    except Exception as e:
        print(f'group by caught {type(e)}: ')
        print(e)
        print("Could not insert into MongoDB")

#consumer = KafkaConsumer('comments',bootstrap_servers=[
 #    'my-kafka-0.my-kafka-headless.er1ck-esp1n0sa.svc.cluster.local:9092'
  #  ])
# Parse received data from Kafka
#for msg in consumer:
 #   record = json.loads(msg.value)
  #  print(record)
   # userId = record["userId"]
   # objectId = record["objectId"]
   # comment = record["comment"]

    # Create dictionary and ingest data into MongoDB
    #try:
     #   comment_rec = {
      #      'userId': userId,
       #     'objectId': objectId,
        #    'comment': comment
        #}
        #print(comment_rec)
        #comment_id = db.socer_comments.insert_one(comment_rec)
        #print("Comment inserted with record ids", comment_id)
    #except Exception as e:
     #   print("Could not insert into MongoDB:")

    # Create bdnosql_sumary and insert groups into mongodb
    #try:
     #   agg_result = db.socer_comments.aggregate([
      #        {
       #  "$group": {
        #        "_id": {
         #           "objectId": "$objectId",
          #          "comment": "$comment"
           #     },
            #    "n": {"$sum": 1}
            #}
       # }
    #])

     #   db.socer_sumaryComments.delete_many({})
      #  for i in agg_result:
       #     print(i)
        #    sumaryComments_id = db.socer_sumaryComments.insert_one(i)
         #   print("Sumary Comments inserted with record ids: ", sumaryComments_id)
    #except Exception as e:
     #   print(f'group vy cought {type(e)}: ')
      #  print(e)