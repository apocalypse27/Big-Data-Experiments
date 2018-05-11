import sqlContext.implicits._
import org.apache.spark.sql.functions._
val movies  = sqlContext.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/movies.csv")
val ratings=sqlContext.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/ratings.csv")
val tags=sqlContext.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/tags.csv")
val movieRatings=movies.as("m").join(ratings.as("r"),$"m.movieId"===$"r.movieId").select($"m.movieId",$"rating").groupBy($"m.movieId").agg(mean($"rating").as("avg"))
val ans1=movieRatings.collect()
val lowestTenRatedMovies=movieRatings.orderBy($"avg").take(10)
val actionMovieRatings=tags.as("t").filter($"t.tag"==="action").join(ratings.as("r"),$"t.movieId"===$"r.movieId").select($"r.movieId",$"t.tag",$"rating").groupBy($"r.movieId").agg(mean($"r.rating")).collect()

display(actionMovieRatings)


val actionMovies=tags.as("t").filter($"tag"==="action")
val thrillerMovies=movies.as("m").filter($"genres".contains("Thriller"))
val actionThrillerAvgRatings=actionMovies.as("a").join(thrillerMovies.as("th"),"movieId").join(ratings.as("r"),"movieId").select($"movieId",$"tag",$"genres",$"rating").groupBy($"movieId").agg(mean($"rating")).collect()

display(actionThrillerAvgRatings)
