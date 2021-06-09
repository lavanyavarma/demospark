import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,udf}

object UDFDemo {
  def main(args :Array[String]): Unit = {

    val spark = SparkSession.builder().appName("filter dataframe").master("local[*]").getOrCreate()
    val df = spark.read.option("header", "true").csv("C:\\Users\\oncall\\Desktop\\LAVANYA\\DATA\\empdata.txt")

    def managerudf(pmanager :String)=if (pmanager!="NULL") "exists" else "not exists"

    val chkmanager=udf(managerudf _)
    df.withColumn("have manager",chkmanager(col("manager"))).show
    

    df.createOrReplaceTempView("emp")
    spark.udf.register("managerudfsql",managerudf _)
    spark.sql("select *,managerudfsql(manager) from emp where designation<>'SALESMAN'").show
  }
}
