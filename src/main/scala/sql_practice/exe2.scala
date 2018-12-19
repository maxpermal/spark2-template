package sql_practice

import spark_helpers.SessionBuilder
import org.apache.spark.sql.functions._

object exe2 {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val salaries07 = spark.read
      .option("sep", "\t")
      .csv("data/input/sample_07")
      .select(
        $"_c0".as("code"),
        $"_c1".as("description"),
        $"_c2".as("total_emp07"),
        $"_c3".as("salary07"))

    val salaries08 = spark.read
      .option("sep", "\t")
      .csv("data/input/sample_08")
      .select(
        $"_c0".as("code"),
        $"_c1".as("description08"),
        $"_c2".as("total_emp08"),
        $"_c3".as("salary08"))

    val salaries08b = salaries08.select($"code",$"salary08",$"total_emp08")
    val salaries07_08 = salaries07.join(salaries08b, Seq("code") )

    //question 2
    val salaries07_GT_100 = salaries07
      .select($"description",$"salary07")
      .filter($"salary07">100000)
    salaries07_GT_100.show

    //question3
    val salaries07_08_growth = salaries07_08
      .select(
        $"description",
        $"salary07",
        $"salary08",
        ( ($"salary08"-$"salary07")/($"salary07") *100).as("growth_percent") )
      .orderBy($"growth_percent".desc)
      .filter($"growth_percent">0)
    salaries07_08_growth.show

    //question 4
    val salaries07_08_jobloss = salaries07_08
      .select(
        $"description",
        $"total_emp07",
        $"total_emp08",
        ( ($"total_emp08"-$"total_emp07")/($"total_emp07") *100).as("jobloss_percent") )
      .orderBy($"jobloss_percent".asc)
      .filter($"jobloss_percent"<0)
    salaries07_08_jobloss.show
  }
}
