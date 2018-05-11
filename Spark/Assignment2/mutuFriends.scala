
import scala.io.Source
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._



val inputFile  = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")

val Friends_Pair = inputFile.map(line=>line.split("\\t")).filter(line => (line.size == 2)).map(line=>(line(0),line(1).split(","))).flatMap(x=>x._2.flatMap(z=>if(x._1.toInt<z.toInt)(List((x._1.toInt,z.toInt)->x._2.toList)) else (List((z.toInt,x._1.toInt)->x._2.toList))))

val y=Friends_Pair.reduceByKey((a,b)=>a.intersect(b))

y.saveAsTextFile("/FileStore/my-stuff/my-file1.txt")

