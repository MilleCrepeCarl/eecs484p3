//query3
//create a collection "cities" to store every user that lives in every city
//Each document(city) has following schema:
/*
{
  _id: city
  users:[userids]
}
*/

function cities_table(dbname) {
    db = db.getSiblingDB(dbname);
    db.createCollection("cities");
    db.cities.insert(
    	db.users.aggregate([{$group:{_id:"$current.city",users:{$addToSet:"$user_id"}}}]).toArray()
    );
}
