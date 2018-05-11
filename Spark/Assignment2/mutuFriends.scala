
import scala.io.Source
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

//dbutils.fs.put("/FileStore/my-stuff/my-file3.txt", "Contents of my file")

val inputFile  = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")
//val Friends_Pair = inputFile.map(line=>line.split("\\t")).filter(line => (line.size == 2)).flatMap(line=>(line(0),line(1).split(","))).collect()

val Friends_Pair = inputFile.map(line=>line.split("\\t")).filter(line => (line.size == 2)).map(line=>(line(0),line(1).split(","))).flatMap(x=>x._2.flatMap(z=>if(x._1.toInt<z.toInt)(List((x._1.toInt,z.toInt)->x._2.toList)) else (List((z.toInt,x._1.toInt)->x._2.toList))))
//val x=sc.parallelize(Friends_Pair)
val y=Friends_Pair.reduceByKey((a,b)=>a.intersect(b)).collect()

//y.saveAsTextFile("/FileStore/my-stuff/my-file1.txt")
//val paralleli=sc.parallelize(y).collect()

//display(y)
for(n<- y) 
{
  if(n._1._1==0 && n._1._2==4){
    print(n._1 + "\t")
    for(i<-n._2)print(i + ",")
    println("")
  }
  if(n._1._1==20 && n._1._2==22939){
    print(n._1 + "\t")
    for(i<-n._2)print(i + ",")
    println("")
  }
  if(n._1._1==6222 && n._1._2==19272){
    print(n._1 + "\t")
    for(i<-n._2)print(i + ",")
    println("")
  }
  if(n._1._1==28041 && n._1._2==28056){
    print(n._1 + "\t")
    for(i<-n._2)print(i + ",")
    println("")
  }
  
}
