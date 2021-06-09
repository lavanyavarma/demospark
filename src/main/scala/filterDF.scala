import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
object filterDF {


  def main(args :Array[String]): Unit ={

    val spark=SparkSession.builder().appName("filter dataframe").master("local[*]").getOrCreate()
    val df=spark.read.option("header","true").csv("C:\\Users\\oncall\\Desktop\\LAVANYA\\DATA\\empdata.txt")
    println(df.printSchema)
df.filter(col("designation")==="SALESMAN").show
    df.where(col("designation")==="SALESMAN").show
df.createOrReplaceTempView("emp_filter")
    spark.sql("select *,'emp_filter' from emp_filter where designation='SALESMAN' ").show
  }

}
