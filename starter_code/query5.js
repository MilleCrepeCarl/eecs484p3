// find the oldest friend for each user who has a friend. 
// For simplicity, use only year of birth to determine age, if there is a tie, use the one with smallest user_id
// return a javascript object : key is the user_id and the value is the oldest_friend id
// You may find query 2 and query 3 helpful. You can create selections if you want. Do not modify users collection.
//
//You should return something like this:(order does not matter)
//{user1:userx1, user2:userx2, user3:userx3,...}

function oldest_friend(dbname){
  db = db.getSiblingDB(dbname);
  // TODO: implement oldest friends
  // return an javascript object described above
  db.users.aggregate(
    [
      {
        $unwind: "$friends" 
      },
      {
        $lookup:
        {
          from:"users",
          localField:"friends",
          foreignField:"user_id",
          as:"u"
        }
      },
      {
        $unwind:"$u"
      }, 
      { 
        $project : 
        { 
          _id : 0,
          u1:"$user_id",
          u2:"$friends",
          y1:"$YOB",
          y2:"$u.YOB"
        } 
      },
      {
        $out : "friends_relation" 
      }
    ]
  );
  var res1=db.friends_relation.aggregate(
    [
      {
        "$sort": { "y2": 1,"u2":1} 
      },
      {
        "$group": 
        {
            "_id": "$u1",
            "age": { "$first": "$y2" },
            "u2": { "$first": "$u2" }
        }
      }
    ]
  ).toArray();
  var res2=db.friends_relation.aggregate(
    [
      {
        "$sort": { "y1": 1,"u1":1 } 
      },
      {
        "$group": 
        {
            "_id": "$u2",
            "age": { "$first": "$y1" },
            "u2": { "$first": "$u1" }
        }
      }
    ]
  ).toArray();
  var result={};
  var tmp={};
  var res=res1.concat(res2);
  res.forEach(
    function(ele)
    {
      if(tmp[ele._id]==null)
      	tmp[ele._id]={age:ele.age,ans:ele.u2};
      else
      {
      	if((ele.age<tmp[ele._id].age)||(ele.age==tmp[ele._id].age)&&(ele.u2<tmp[ele._id].ans))
        	tmp[ele._id]={age:ele.age,ans:ele.u2};
      }
    }
  );
  Object.keys(tmp).forEach(
    function(ele)
    {
      result[ele]=tmp[ele].ans;
    }
  );
  return result
}
