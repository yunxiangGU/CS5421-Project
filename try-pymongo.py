# trying out PyMongo
import pprint
import datetime

import pymongo

# MAKING A CONNECTION
client = pymongo.MongoClient("localhost", 27017)
# or
# client = MongoClient('mongodb://localhost:27017/')

# GETTING A DATABASE
db = client.test_database
# or
# db = client['test-database']

# GETTING A COLLECTION
collection = db.test_collection
# or
# collection = db['test_collection']

# INSERTING A DOCUMENT
post = {"author": "Mike",
        "text": "My first blog post!",
        "tags": ["mongodb", "python", "pymongo"],
        "date": datetime.datetime.utcnow()}
posts = db.posts
post_id = posts.insert_one(post).inserted_id
print(post_id)
print(db.list_collection_names())

# GETTING A DOCUMENT WITH find_one()
pprint.pprint(posts.find_one())
pprint.pprint(posts.find_one({"author": "Mike"}))
pprint.pprint(posts.find_one({"author": "Eliot"}))

# QUERYING BY ObjectId
pprint.pprint(posts.find_one({"_id": post_id}))
# note that an ObjectId is not the same as its string representation
post_id_as_str = str(post_id)
pprint.pprint(posts.find_one({"_id": post_id_as_str}))  # No result

# BULK INSERTS
new_posts = [{"author": "Mike",
              "text": "Another post!",
              "tags": ["bulk", "insert"],
              "date": datetime.datetime(2009, 11, 12, 11, 14)},
             {"author": "Eliot",
              "title": "MongoDB is fun",
              "text": "and pretty easy too!",
              "date": datetime.datetime(2009, 11, 10, 10, 45)}]
result = posts.insert_many(new_posts)
print(result.inserted_ids)

# QUERYING FOR MORE THAN ONE DOCUMENT
for post in posts.find():
    pprint.pprint(post)
for post in posts.find({"author": "Mike"}):
    pprint.pprint(post)

# COUNTING
print(posts.count_documents({}))
print(posts.count_documents({"author": "Mike"}))

# RANGING QUERIES
d = datetime.datetime(2009, 11, 12, 12)
for post in posts.find({"date": {"$lt": d}}).sort("author"):
    pprint.pprint(post)

# INDEXING
result = db.profiles.create_index([('user_id', pymongo.ASCENDING)], unique=True)
print(sorted(list(db.profiles.index_information())))
