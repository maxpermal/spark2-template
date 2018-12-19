package sql_practice


import spark_helpers.SessionBuilder
import org.apache.spark.sql.functions._

object exe3 {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")

    //question 1
    printf(">>>>>>>>>>>>>>>>>>>>>>question 1\n")
    val levelsDifficulties = toursDF.select($"tourDifficulty").distinct()
    levelsDifficulties.show
    printf(">>>>>>>>>>>>>>>>>>>>>>Levels of difficulties : "+levelsDifficulties.count + "\n")

    //question 2
    printf(">>>>>>>>>>>>>>>>>>>>>>question 2\n")
    val minPrice = toursDF.select($"tourPrice").agg(min($"tourPrice"),max($"tourPrice"),mean($"tourPrice"))
    minPrice.show()

    //question 3
    printf(">>>>>>>>>>>>>>>>>>>>>>question 3\n")
    toursDF.groupBy($"tourDifficulty").agg(min($"tourPrice"),max($"tourPrice"),mean($"tourPrice")).show

    //question 4
    printf(">>>>>>>>>>>>>>>>>>>>>>question 4\n")
    val top_tourDiff = toursDF
      .groupBy($"tourDifficulty").agg(min($"tourPrice"),max($"tourPrice"),mean($"tourPrice"),
        min($"tourLength"),max($"tourLength"),mean($"tourLength"))
    top_tourDiff.show

    //question 5
    printf(">>>>>>>>>>>>>>>>>>>>>>question 5\n")
    val top10_tourTags = toursDF
      .select(explode($"tourTags").as("tourTag"),$"tourDifficulty")
      .groupBy($"tourTag")
      .count()
      .orderBy($"count".desc).limit(10)
    top10_tourTags.show

    //question 6
    printf(">>>>>>>>>>>>>>>>>>>>>>question 6\n")
    val top10_tourTagsDiff = toursDF
      .select(explode($"tourTags").as("tourTag"),$"tourDifficulty")
      .groupBy($"tourTag",$"tourDifficulty")
      .count()
      .orderBy($"tourTag".asc).limit(10)
    top10_tourTagsDiff.show

    //question 7
    printf(">>>>>>>>>>>>>>>>>>>>>>question 7\n")
    val tourPriceTagsDiff = toursDF
      .select(explode($"tourTags").as("tourTag"),$"tourDifficulty",$"tourPrice")
      .groupBy($"tourTag",$"tourDifficulty")
      .agg(mean("tourPrice").as("average"),min("tourPrice"),max("tourPrice"))
      .orderBy($"average".desc)
    tourPriceTagsDiff.show(1000)
  }
}
