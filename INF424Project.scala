import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, desc, pow, sum}
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.{FileSystem, Path}

object INF424Project extends App {

  val spark = SparkSession.builder
    .appName("MoviePreferenceAnalyzer")
    .master("yarn")
    .config("spark.hadoop.fs.defaultFS", "hdfs://clu01.softnet.tuc.gr:8020")
    //.config("spark.yarn.jars", "hdfs://clu01.softnet.tuc.gr:8020/user/xenia/jars/*.jar")
    .config("spark.hadoop.yarn.resourcemanager.address", "http://clu01.softnet.tuc.gr:8189")
    .config("spark.hadoop.yarn.application.classpath", "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*")
    //.enableHiveSupport()
    .getOrCreate()


//    val spark = SparkSession.builder
//      .master("local[*]") //local[*] means "use as many threads as the number of processors available to the Java virtual machine"
//      .appName("")
//      .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
//      .getOrCreate()


  //==================================== PART A ===============================================================

  val inputPath = "hdfs://localhost:9000/ml-latest/"
  val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)


  //hdfs://clu01.softnet.tuc.gr:8020/user/chrisa/
  //hdfs://localhost:9000/
  val movies = "hdfs://clu01.softnet.tuc.gr:8020/user/chrisa/ml-latest/movies.csv"
  val ratings = "hdfs://clu01.softnet.tuc.gr:8020/user/chrisa/ml-latest/ratings.csv"
  val tags = "hdfs://clu01.softnet.tuc.gr:8020/user/chrisa/ml-latest/tags.csv"
  val genscores = "hdfs://clu01.softnet.tuc.gr:8020/user/chrisa/ml-latest/genome-scores.csv"
  val gentags = "hdfs://clu01.softnet.tuc.gr:8020/user/chrisa/ml-latest/genome-tags.csv"

  val movieRddRaw = spark.sparkContext.textFile(movies)
  //val movieHeader = movieRddRaw.first()
  val movieRdd = movieRddRaw.filter(!_.startsWith("movieId"))

  val tagRddRaw = spark.sparkContext.textFile(tags)
  //val tagHeader = tagRddRaw.first()
  val tagRdd = tagRddRaw.filter(!_.startsWith("userId"))

  val ratingsRddRaw = spark.sparkContext.textFile(ratings)
  //val ratingsHeader = ratingsRddRaw.first()
  val ratingsRdd = ratingsRddRaw.filter(!_.startsWith("userId"))




  //****************************************** query 2 ************************************************************************

  val movieGenres = movieRdd.map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).flatMap(x =>
      x(2).split("\\|").map(genre => (x(0), genre)))
    .filter(_._2 != "(no genres listed)") // (movieId, genre)


  val movieTag = tagRdd.map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).map(x =>
    (x(0),x(2))) //(movie, tag)

  val movieRating = ratingsRdd.map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).map(x =>
    (x(0), x(2).toDouble)) //(movieId, rating)

  val genretag = movieTag.join(movieGenres).map(x => ((x._2._2, x._2._1), 1)) // (genre,tag) , 1)
    .reduceByKey(_ + _) //  (genre,tag) counter)
    .map(x => (x._1._1, (x._1._2, x._2)))  //  (genre)(tag,counter).
    .groupByKey()
    .mapValues(_.toList.sortBy(_._2).reverse.head) // find the max counter aka most used tag
    .sortBy(_._2._2, ascending = false)             //return genres based on the most frequent tag
  //(genre)(tag, maxcount)


  val tagGenreId = movieTag.join(movieGenres).map(x => (x._2._2, x._1)) // ((genre), id)

  val avgTopTagMoviesRating = genretag.join(tagGenreId).map(x => (x._2._2, (x._1 ,x._2._1._1)))// (movieids,(genre,tag)) for the most frequent tag
    .join(movieRating).map{
      x =>  ((x._2._1._1, x._2._1._2), (x._2._2, 1))}  // (genre, tag) (rating,1)
    .reduceByKey((x,y) => ((x._1 + y._1), (x._2 + y._2))) //) (genre,tag) (sum, count)
    .mapValues(x => x._1 / x._2)
    .map( x => (x._1._1, x._1._2, x._2)) // (genre, tag, avg)
    .sortBy(_._3, ascending = false)

  // avgTopTagMoviesRating.take(20).foreach(println)


  hdfs.delete(new Path("hdfs://clu01.softnet.tuc.gr:8020/user/fp25_5/Query2SampleOutput"), true)
  val q1output = spark.sparkContext.parallelize(avgTopTagMoviesRating.take(50))
  q1output.coalesce(1).saveAsTextFile("hdfs://clu01.softnet.tuc.gr:8020/user/fp25_5/Query2SampleOutput") // store


  //*****************************************************************************************************************************


  //****************************************** query 4 ******************************************************************************

  val tagUser = tagRdd.map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).map(x =>
    ((x(0),x(1)), x(2))) // ((userId, movieId), tag)

  val ratingUser = ratingsRdd.map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).map( x =>
    ((x(0), x(1)), x(2).toDouble)) //((userId, movieId), rating)

  val avgTagRating = tagUser.join(ratingUser).map(x => ((x._2._1), (x._2._2, 1))) // (tag,(rating,1))
    .reduceByKey((x,y) => ((x._1 + y._1), (x._2 + y._2))) // (tag) (sum, count)
    .mapValues(x => x._1 / x._2)
    .sortBy(_._2, ascending = false) //  tag , avg

  //avgTagRating.take(50).foreach(println)

  hdfs.delete(new Path("hdfs://clu01.softnet.tuc.gr:8020/user/fp25_5/Query4SampleOutput"), true)
  val q4output = spark.sparkContext.parallelize(avgTagRating.take(50))
  q4output.coalesce(1).saveAsTextFile("hdfs://clu01.softnet.tuc.gr:8020/user/fp25_5/Query4SampleOutput") // store

  //*****************************************************************************************************************************

  //==============================================================================================================================

  //==============================================PART B==========================================================================

  val moviedf = spark.read.format("csv")
    .option("sep", ",")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs://clu01.softnet.tuc.gr:8020/user/chrisa/ml-latest/movies.csv")


  val ratingsdf = spark.read.format("csv")
    .option("sep", ",")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs://clu01.softnet.tuc.gr:8020/user/chrisa/ml-latest/ratings.csv")


  val tagsdf = spark.read.format("csv")
    .option("sep", ",")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs://clu01.softnet.tuc.gr:8020/user/chrisa/ml-latest/tags.csv")

  val genscoresdf = spark.read.format("csv")
    .option("sep", ",")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs://clu01.softnet.tuc.gr:8020/user/chrisa/ml-latest/genome-scores.csv")

  //****************************************** query 6 ************************************************************************

  val ratingStats = ratingsdf.groupBy(col("movieId"))
    .agg(avg("rating").alias("avgRating"),                        //(movieId, (avgrating, counter)
      count("rating").alias("ratingCount")
    )

  val avgTagRel = genscoresdf.groupBy(col("movieId"))             //(movieId, avgTagRelevance)
    .agg(avg("relevance").alias("avgTagRelevance"))

  val Q6data = ratingStats.join(avgTagRel, ratingStats("movieId") === avgTagRel("movieId")).drop(avgTagRel("movieId")) // (movieId, (avgRating, counter, avgTagRelevance)

  val skylineRatGenSc = Q6data.alias("t1").join(Q6data.alias("t2"),
    col("t1.avgRating") >= col("t2.avgRating") &&
      col("t1.ratingCount") >= col("t2.ratingCount") &&
      col("t1.avgTagRelevance") >= col("t2.avgTagRelevance") &&
      (
        col("t2.avgRating") > col("t1.avgRating") ||
          col("t2.ratingCount") > col("t1.ratingCount") ||
          col("t2.avgTagRelevance") > col("t1.avgTagRelevance")
        )
    , "left_anti"
  )

  //   hdfs://localhost:9000/Query6SampleOutput
  //   hdfs://clu01.softnet.tuc.gr:8020/user/fp25_5
  hdfs.delete(new Path("hdfs://clu01.softnet.tuc.gr:8020/user/fp25_5/Query6SampleOutput"), true)
  skylineRatGenSc.limit(50).coalesce(1).write.mode("overwrite").format("csv").save("hdfs://clu01.softnet.tuc.gr:8020/user/fp25_5/Query6SampleOutput")

  //skylineRatGenSc.show()
  //*****************************************************************************************************************************


  //****************************************** query 8 **************************************************************************
  //movie target = 4011 (Snatch)

  val movieVector = genscoresdf.filter(col("movieId") === "4011").select(
    col("tagId").as("Tag"),
    col("relevance").as("RelevanceOfMyMovie")
  )

  val userGenresscores = genscoresdf.select(
    col("movieId"),
    col("tagId"),
    col("relevance")
  )

  val ratingSample = ratingsdf.limit(30000)

  val usersVector = ratingSample.filter(col("rating") > 4.0).select(
      col("userId"),
      col("movieId")
    ).join(userGenresscores, "movieId").groupBy("userId","tagId")
    .agg(avg("relevance").alias("usersRelevance"))                                        //(userId,tagId) (avg Relevance)

  val cosSim = movieVector.join(usersVector, movieVector("Tag") === usersVector("tagId")).groupBy("userId")
    .agg(
      sum(col("RelevanceOfMyMovie") * col("usersRelevance")).alias("numerator"),
      pow(sum((col("RelevanceOfMyMovie")*col("RelevanceOfMyMovie"))),0.5).alias("denominator1"),
      pow(sum((col("usersRelevance")*col("usersRelevance"))),0.5).alias("denominator2")
    )
    .select(
      col("userId"),
      (col("numerator") / (col("denominator1") * col("denominator2"))).alias("Cosine Similarity")
    )
    .orderBy(col("Cosine Similarity").desc)

  //cosSim.show()

  //hdfs://localhost:9000
  hdfs.delete(new Path("hdfs://clu01.softnet.tuc.gr:8020/user/fp25_5/Query8SampleOutput"), true)
  cosSim.coalesce(1).write.mode("overwrite").format("csv").save("hdfs://clu01.softnet.tuc.gr:8020/user/fp25_5/Query8SampleOutput")
  //*****************************************************************************************************************************

  //****************************************** query 10 *************************************************************************
  //movie target = 2542 (Lock, Stock & Two Smoking Barrels)

  val movieVector10 = genscoresdf.filter(col("movieId") === "2542").select(
    col("tagId").as("Tag"),
    col("relevance").as("RelevanceOfMyMovie")
  )

  val ratingSample10 = ratingsdf.limit(30000)


  val usersvector10 = ratingSample10.filter(col("rating") > 4.0).select(
      col("userId"),
      col("movieId")
    ).join(genscoresdf, ratingSample10("movieId") === genscoresdf("movieId")).select(
      col("userId"),
      col("tagId"),
      col("relevance")
    ).groupBy("userId","tagId")
    .agg(avg("relevance").alias("usersRelevance"))

  val cosSim10 = movieVector10.join(usersvector10, movieVector10("Tag") === usersvector10("tagId")).groupBy("userId")
    .agg(
      sum(col("RelevanceOfMyMovie") * col("usersRelevance")).alias("numerator"),
      pow(sum((col("RelevanceOfMyMovie")*col("RelevanceOfMyMovie"))),0.5).alias("denominator1"),
      pow(sum((col("usersRelevance")*col("usersRelevance"))),0.5).alias("denominator2")
    )
    .select(
      col("userId"),
      (col("numerator") / (col("denominator1") * col("denominator2"))).alias("Cosine Similarity")
    )
    .orderBy(col("Cosine Similarity").desc)
    .limit(3)

  hdfs.delete(new Path("hdfs://clu01.softnet.tuc.gr:8020/user/fp25_5/Query10SampleOutput"), true)
  cosSim10.coalesce(1).write.mode("overwrite").format("csv").save("hdfs://clu01.softnet.tuc.gr:8020/user/fp25_5/Query10SampleOutput")
  //*****************************************************************************************************************************


  spark.stop()
}


