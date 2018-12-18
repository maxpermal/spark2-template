package sql_practice

import spark_helpers.SessionBuilder
import org.apache.spark.sql.functions._

object exe1 {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    //question 1
    val demoDF = spark.read
      .option("mode", "PERMISSIVE")
      .json("data/input/demographie_par_commune.json")
    demoDF.select($"Commune",$"Population").agg(sum($"Population")).show

    //question 2
    val demoDFClasse=demoDF.select($"Departement".as("dep"), $"Population".as("pop"))
      .groupBy($"dep")
      .agg(sum("pop").as("tot"))
      .orderBy($"tot".desc)
    demoDFClasse.show

    //question 3
    val depDF = spark.read
      .option("mode", "PERMISSIVE")
      .csv("data/input/departements.txt").select( $"_c0".as("name") , $"_c1".as("dep") )
    val demoDepDF = demoDFClasse.join(depDF, Seq("dep") )
    demoDepDF.show

  }
}
