# ==============================================================================
# title           : MongoDB.py
# description     : MongoDB to get data.
# author          : ojebode@gmail.com
# date            : 11-06-21
# python_version  : 3.7
# ==============================================================================

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson import ObjectId



class MongoDBException(Exception):
    pass


# Methods to perform MongoDB.operations
class MongoDBManager(object):

    def __init__(self, integration_conf):
        
        self.username = integration_conf['Username']
        self.password = integration_conf['Password']
        self.server = integration_conf['Server Address']
        self.port = int(integration_conf['Port'])
        self.use_auth = integration_conf['Use Authentication'].lower() == "true"

# Connect to mongoDB
        self.client = MongoClient(self.server, self.port, connect=True)
        
        
#Method to Authenticate user    
    def authenticate(self, db):
        try:
            self.client = MongoClient(host=self.server, port=self.port, username=self.username, password=self.password, authSource=db)
            return self.client
            
        except Exception as e:
            raise MongoDBException(e)


# Method to Check if the connection is established or not.
    def test_connectivity(self):
        try:
            # Forces a call.
            self.client.server_info()
            return True
        except ConnectionFailure as e:
            raise MongoDBException("Server not available. {0}".format(e.message))


# Method to Check if collection exist in database
    def validate_collection(self, collection_name, database_name):
        database = self.client[database_name]
        collection_names = database.collection_names()
        if collection_name in collection_names:
            return True
        raise MongoDBException("Collection {0} NOT exist in {1} database. Please try again.".format(collection_name, database_name))


# Method to Check if specific database exist
    def validate_database(self, database_name):
        databases = self.client.database_names()
        if database_name in databases:
            return True
        raise MongoDBException("Database {0} NOT exist. Please try again.".format(database_name))


# Method to Query MongoDB for documents.
    def execute_query(self, query, database_, collection_):

        try:
            data = []
            
            # Check if Authentication required
            if self.use_auth:
                client = self.authenticate(database_)
                
                self.validate_database(database_)
                database = client[database_]
                
                # Check if collection exist in db
                self.validate_collection(collection_, database_)
                collection = database[collection_]
                
            else:
                # Check if database exist
                self.validate_database(database_)
                database = self.client[database_]
    
                # Check if collection exist in db
                self.validate_collection(collection_, database_)
                collection = database[collection_]


            # Query the database. Returns a Cursor instance
            results = collection.find(query)
            # doc = record
            for doc in results:
                for field_value in doc:
                    # ObjectId is not JSON serializable
                    if isinstance(doc[field_value], ObjectId):
                        doc[field_value] = str(doc[field_value])
                data.append(doc)

            return data

        except Exception as e:
            raise MongoDBException(e)


# Method to insert documents into MongoDB.
    def insert_documents(self, database, collection, document):
        
        # Check if Authentication required
        if self.use_auth:
            conn = self.authenticate(database)
            db = conn[database]
            
            val_db = self.validate_database(database)
            val_col = self.validate_collection(collection, database)
            
        else:
            conn = self.client
            db = conn[database]
            
            val_db = self.validate_database(database)
            val_col = self.validate_collection(collection, database)
            

        if val_db and val_col:
            try:
                if isinstance(document, list):
                    add_method = db[collection].insert_many
                    res = add_method(document)
                    ids = res.inserted_ids
                    return res, ids
                elif isinstance(document, dict):
                    add_method = db[collection].insert_one
                    res = add_method(document)
                    ids = res.inserted_id
                    return res, ids
                else:
                    raise MongoDBException("Input data must be either dict or list")
            
            except Exception as err:
                raise MongoDBException(err)
                
                
# Method to update documents in MongoDB.
    def update_documents(self, database, collection, query, document):
        # Check if Authentication required
        if self.use_auth:
            conn = self.authenticate(database)
            db = conn[database]
            
            val_db = self.validate_database(database)
            val_col = self.validate_collection(collection, database)
            
        else:
            conn = self.client
            db = conn[database]
            
            val_db = self.validate_database(database)
            val_col = self.validate_collection(collection, database)
        
        if val_db and val_col:
            try:
                result = db[collection].update_many(query, document)
                return result
            except Exception as err:
                raise MongoDBException(err)


# Method to delete documents in MongoDB.
    def delete_documents(self, database, collection, query):
        # Check if Authentication required
        if self.use_auth:
            conn = self.authenticate(database)
            db = conn[database]
            
            val_db = self.validate_database(database)
            val_col = self.validate_collection(collection, database)
            
        else:
            conn = self.client
            db = conn[database]
            
            val_db = self.validate_database(database)
            val_col = self.validate_collection(collection, database)
        
        if val_db and val_col:
            try:
                result = db[collection].delete_many(query)
                return result
            except Exception as err:
                raise MongoDBException(err)


    def close_connection(self):
        """
        Close the connection
        """
        self.client.close()


