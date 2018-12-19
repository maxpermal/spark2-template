import spark_helpers.SessionBuilder

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SessionBuilder.buildSession()

    val sparkVersion = spark.version
    println(s"Spark Version: $sparkVersion")

//    sql_practice.exe1.exec1()
//    sql_practice.exe2.exec1()
    sql_practice.exe3.exec1()
  }
}
