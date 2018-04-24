import pprint
import pymongo
from pymongo import MongoClient
from bson.son import SON
import pandas as pd
import json

STEP = 200


def query1(database):
    query1 = [
        {"$group":{"_id":"$DayOfWeek","number":{"$sum":1}}}
    ]
    pprint.pprint("Number of incidents per day in week")
    pprint.pprint(list(database.aggregate(query1)))

def query2(database):
    query2 = [
        {"$group": {"_id":"$PdDistrict", "number": {"$sum": 1}}},
        {"$sort": {"number": -1}},
        {"$limit": 1}
    ]
    pprint.pprint("Whis district is most dangerous?")
    pprint.pprint(list(database.aggregate(query2)))

def query3(database):
    query3 = [
        {"$group": {"_id":"$Category","number":{"$sum":1}}},
        {"$sort": {"number":-1}},
        {"$limit":1}
    ]
    pprint.pprint("What is the most common incidents?")
    pprint.pprint(list(database.aggregate(query3)))

def query4(database):
    from bson.code import Code
    mapper = Code("""
               function () {
                if(this.Category==\"ROBBERY\"){
                   emit(this.PdDistrict, 1);
                 };
               }
            """)
    reducer = Code("""
                function (key, values) {
                  var total = 0;
                  for (var i = 0; i < values.length; i++) {
                    total += values[i];
                  }
                  return total;
                }
                """)



    result = database.map_reduce(mapper, reducer, "myresults")

    pprint.pprint("Which district has the most ROBBERIES?")
    query4= [
        {"$sort": {"value": -1}},
        {"$project": {"_id": 1}},
        {"$limit": 1}
    ]
    pprint.pprint(list(result.aggregate(query4)))

if __name__ == "__main__":
    client = MongoClient()
    # Creates the database for the San Francisco city incidents data
    db = client['datascience']
    # Creates the collection of the documents of that will represent the incidents
    incidents = db.incidents
    # We delete any content of the incidents collection, just in case it has anything
    incidents.remove()

    # Reads the csv file into python's dataframe type (in my case I named it incid.csv)
    csv = pd.read_csv('C:/Users/Michaela/Documents/Michaela/IngeneriaYCiencesDeDatos2/DataSanFrancisco.csv')

    for start in range(0, len(csv.index), STEP):
        partial_csv = csv.ix[start:(start + STEP), :]

        # Reads the dataframe as json using orient=records
        csv_to_json = json.loads(partial_csv.to_json(orient='records'))

        # We bulk all data in
        incidents.insert(csv_to_json)

        # we free some memory as csv is not needed anymore
        del partial_csv
        del csv_to_json

    query1(incidents)
    query2(incidents)
    query3(incidents)
    query4(incidents)
