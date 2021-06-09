import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object wordCount {
def main(args: Array[String]): Unit ={

  /*val conf=new SparkConf().setMaster("local[*]").setAppName("test demo")
  val sc=new SparkContext(conf)*/
  val spark=SparkSession.builder().appName("word count program").master("local[*]").getOrCreate()
  val sc=spark.sparkContext
  val rdd=sc.textFile("C:\\Users\\oncall\\Desktop\\LAVANYA\\DATA\\empdata.txt")
  println("count is "+rdd.count)
  val wordrdd= rdd.flatMap(line=>line.split(",")).map(word=>(word,1))
  val wordcountrdd = wordrdd.reduceByKey(_+_)
  wordcountrdd.foreach(println)
  val filterrdd= rdd.flatMap(line=>line.split(",")).filter(x=>x.length>4)
  filterrdd.foreach(println)
}
}
