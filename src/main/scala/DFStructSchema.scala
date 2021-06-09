import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object DFStructSchema {
  def main(args :Array[String]): Unit ={

    val spark=SparkSession.builder().master("local[*]").appName("DF struct schema").getOrCreate()
    val schema=new StructType(Array(StructField("empno",IntegerType,true),
      StructField("ename",StringType,true),
      StructField("designation",StringType,true),
      StructField("manager",IntegerType,true),
      StructField("hire_date",TimestampType,true),
      StructField("sal",DoubleType,true),
      StructField("deptno",IntegerType,true)

    ))
    val df1=spark.read.option("header","true").csv("C:\\Users\\oncall\\Desktop\\LAVANYA\\DATA\\empdata.txt")
    val df2=spark.read.option("header","true").option("inferschema","true").csv("C:\\Users\\oncall\\Desktop\\LAVANYA\\DATA\\empdata.txt")
    val df3=spark.read.option("header","true").schema(schema).csv("C:\\Users\\oncall\\Desktop\\LAVANYA\\DATA\\empdata.txt")

    println(df1.printSchema())
    println(df2.printSchema())
    println(df3.printSchema())
    df3.show


  }

}
