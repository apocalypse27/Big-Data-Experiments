import scala.collection.Searching._
val friendsList  = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")
val userData=sc.textFile("/FileStore/tables/userdata.txt")

val Friends_Pair = friendsList.map(line=>line.split("\\t")).filter(line => (line.size == 2)).map(line=>(line(0),line(1).split(","))).flatMap(x=>x._2.flatMap(z=>if(x._1.toInt<z.toInt)(List((x._1.toInt,z.toInt)->x._2.toList)) else (List((z.toInt,x._1.toInt)->x._2.toList))))
val personalDetails=userData.map(line=>line.split(",")).map(x=>(x(0),Map(("FirstName",x(1)),("LastName",x(2)),("Address",x(3)+","+x(4)+","+x(5) + ","+x(6)+","+x(7)))))

val y=Friends_Pair.reduceByKey((a,b)=>a.intersect(b))
val countFriends=y.map(pair=>(pair._1,pair._2.length)).sortBy(_._2,false).take(10)
val parallelized=sc.parallelize(countFriends)

//val trial=parallelized.map(x=>(x._2,(""+x._1._1,x._2).join(personalDetails)))*/
val firstFriend=parallelized.map(x=>(""+x._1._1,x._2))
val secondFriend=parallelized.map(x=>(""+x._1._2,x._2))

val joinedValues1=firstFriend.join(personalDetails)
val firstPersonList=joinedValues1.map(x=>x._1.toInt).collect()
val firstPersonDetails=joinedValues1.map(y=>y._2._2).collect()

val joinedValues2=secondFriend.join(personalDetails)
val secondPersonList=joinedValues2.map(x=>x._1.toInt).collect()
val secondPersonDetails=joinedValues2.map(y=>y._2._2).collect()

for(pair<-countFriends)
{
  println("")
  print(pair._2 + "\t")
  val at=firstPersonList.indexWhere( _ == pair._1._1)
  
  //print(firstPersonDetails(at) + "\t")
  for ((k,v) <- firstPersonDetails(at)) printf("%s\t", v)
  
  val at2=secondPersonList.indexWhere(_==pair._1._2)
  for ((k,v) <- secondPersonDetails(at2)) printf("%s\t", v)
}
