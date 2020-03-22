
// query 4: find user pairs (A,B) that meet the following constraints:
// i) user A is male and user B is female
// ii) their Year_Of_Birth difference is less than year_diff
// iii) user A and B are not friends
// iv) user A and B are from the same hometown city
// The following is the schema for output pairs:
// [
//      [user_id1, user_id2],
//      [user_id1, user_id3],
//      [user_id4, user_id2],
//      ...
//  ]
// user_id is the field from the users collection. Do not use the _id field in users.

function suggest_friends(year_diff, dbname) {
    db = db.getSiblingDB(dbname);
    var pairs = [];
    // TODO: implement suggest friends
    // Return an array of arrays.
	db.users.find({gender: "male"}).forEach(function(u1) {
		db.users.find({"gender": "female", "hometown.city": u1.hometown.city, "YOB": {$gt: u1.YOB - year_diff, $lt: u1.YOB + year_diff}}).forEach(function(u2) {
			if (!db.flat_users.find({"user_id": Math.min(u1.user_id, u2.user_id), "friends": Math.max(u1.user_id, u2.user_id)}).hasNext()) pairs.push([u1.user_id, u2.user_id]);
		});
	});
    return pairs;
}
