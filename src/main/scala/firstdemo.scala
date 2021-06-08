import org.apache.spark.{SparkConf,SparkContext}
import scala.io.Source

object firstdemo {

  def main(args: Array[String] ): Unit ={

    println("hello world hi");

      val conf=new SparkConf().setMaster("local[*]").setAppName("test demo")

     val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd=sc.parallelize(List(1,2,3))

    println(rdd.reduce(_+_))

    var configMap=Source.fromFile("C:\\bigdata\\data\\properties.txt").getLines()
      .filter(line => line.contains("=")).map{ line => val tkns=line.split("=")
      if(tkns.size==1){
        (tkns(0) -> "" )
      }else{
        (tkns(0) ->  tkns(1))
      }
    }.toMap
      println(configMap("url"))
    }
}