# trying out motor with asyncio
import pprint
import motor.motor_asyncio
import asyncio
from bson import SON

# CREATING A CLIENT
client = motor.motor_asyncio.AsyncIOMotorClient('localhost', 27017)
# or connect with URI
# client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
# or connect to a replica set
# client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://host1,host2/?replicaSet=my-replicaset-name')

# GETTING A DATABASE
db = client.test_database
# alternatively
# db = client['test_database']

# GETTING A COLLECTION
collection = db.test_collection
# or
# collection = db['test_collection']


# INSERTING A DOCUMENT
async def do_insert():
    document = {'key': 'value'}
    result = await db.test_collection.insert_one(document)
    print('result %s' % repr(result.inserted_id))

loop = asyncio.get_event_loop()
loop.run_until_complete(do_insert())


# INSERTING IN BATCHES
async def do_insert():
    result = await db.test_collection.insert_many(
        [{'i': i} for i in range(2000)])
    print('inserted %d docs' % (len(result.inserted_ids),))

loop = asyncio.get_event_loop()
loop.run_until_complete(do_insert())


# GETTING A SINGLE DOCUMENT
async def do_find_one():
    document = await db.test_collection.find_one({'i': {'$lt': 1}})
    pprint.pprint(document)

loop = asyncio.get_event_loop()
loop.run_until_complete(do_find_one())


# QUERYING FOR MORE THAN ONE DOCUMENT
async def do_find():
    cursor = db.test_collection.find({'i': {'$lt': 5}}).sort('i')
    for document in await cursor.to_list(length=100):
        pprint.pprint(document)

loop = asyncio.get_event_loop()
loop.run_until_complete(do_find())


# async for loop
async def do_find():
    c = db.test_collection
    async for document in c.find({'i': {'$lt': 2}}):
        pprint.pprint(document)

loop = asyncio.get_event_loop()
loop.run_until_complete(do_find())


async def do_find():
    cursor = db.test_collection.find({'i': {'$lt': 4}})
    # Modify the query before iterating
    cursor.sort('i', -1).skip(1).limit(2)
    async for document in cursor:
        pprint.pprint(document)

loop = asyncio.get_event_loop()
loop.run_until_complete(do_find())


# COUNTING DOCUMENTS
async def do_count():
    n = await db.test_collection.count_documents({})
    print('%s documents in collection' % n)
    n = await db.test_collection.count_documents({'i': {'$gt': 1000}})
    print('%s documents where i > 1000' % n)

loop = asyncio.get_event_loop()
loop.run_until_complete(do_count())


# UPDATING DOCUMENTS
async def do_replace():
    coll = db.test_collection
    old_document = await coll.find_one({'i': 50})
    print('found document: %s' % pprint.pformat(old_document))
    _id = old_document['_id']
    result = await coll.replace_one({'_id': _id}, {'key': 'value'})
    print('replaced %s document' % result.modified_count)
    new_document = await coll.find_one({'_id': _id})
    print('document is now %s' % pprint.pformat(new_document))

loop = asyncio.get_event_loop()
loop.run_until_complete(do_replace())


async def do_update():
    coll = db.test_collection
    result = await coll.update_one({'i': 51}, {'$set': {'key': 'value'}})
    print('updated %s document' % result.modified_count)
    new_document = await coll.find_one({'i': 51})
    print('document is now %s' % pprint.pformat(new_document))

loop = asyncio.get_event_loop()
loop.run_until_complete(do_update())


# DELETING DOCUMENTS
async def do_delete_many():
    coll = db.test_collection
    n = await coll.count_documents({})
    print('%s documents before calling delete_many()' % n)
    result = await db.test_collection.delete_many({'i': {'$gte': 1000}})
    print('%s documents after' % (await coll.count_documents({})))

loop = asyncio.get_event_loop()
loop.run_until_complete(do_delete_many())


# RUNNING COMMANDS
async def use_distinct_command():
    response = await db.command(SON([("distinct", "test_collection"),
                                     ("key", "i")]))

loop = asyncio.get_event_loop()
loop.run_until_complete(use_distinct_command())
