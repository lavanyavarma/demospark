import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,explode}


object readJSONFile {
  def main(args :Array[String]): Unit = {
    val spark = SparkSession.builder.appName("read avro file").master("local").getOrCreate()

    val df=spark.read.option("multiline","true").json("C:\\Users\\oncall\\Desktop\\LAVANYA\\DATA\\empdataML.json")

    val nesteddf = spark.sqlContext.read.json("C:\\Users\\oncall\\Desktop\\LAVANYA\\DATA\\nested.json")

  /*  val flatteneddf = nesteddf.withColumn("lang", explode(col("lang")))
      .withColumn("id", col("lang"(0)))
      .withColumn("langs", col("lang"(1)))
      .withColumn("type", col("lang"(2)))
      .drop("lang")
      .withColumnRenamed("langs", "lang")
      .show(false)*/

    df.show
    nesteddf.show
   // flatteneddf.show

  }
}
