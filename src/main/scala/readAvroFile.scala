import org.apache.spark.sql.SparkSession

object readAvroFile {
def main(args :Array[String]): Unit ={
  val spark=SparkSession.builder.appName("read avro file").master("local").getOrCreate()

  //val df=spark.read.format("com.databricks.avro").load("C:\\Users\\oncall\\Desktop\\LAVANYA\\DATA\\userdata1.avro") // prior to spark 2.4

 // val df=spark.read.format("avro").load("C:\\Users\\oncall\\Desktop\\LAVANYA\\DATA\\userdata1.avro") // Since 2.4

  //df.write.format("avro").save("C:\\Users\\oncall\\Desktop\\LAVANYA\\DATA\\OUTPUT\\saveavrofile")
  spark.sqlContext.sql(" CREATE TEMPORARY VIEW PERSON USING avro OPTIONS (path \"C:/Users/oncall/Desktop/LAVANYA/DATA/OUTPUT/test.avro\")")
  spark.sqlContext.sql("select * from PERSON").show




}
}
