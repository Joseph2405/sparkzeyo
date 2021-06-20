package sparkpack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.Row 
import org.apache.spark.sql.SparkSession 
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.functions._

object sparkzeyoObj {
  case class schema(txnno:String, txndate:String, custno:String, amount:String,category:String, product:String, city:String, state:String, spendby:String)
  
  def main(args:Array[String]):Unit={ 


			val conf=new SparkConf().setAppName("spark_integration").setMaster("local[*]")
				val sc=new SparkContext(conf)
				sc.setLogLevel("Error")
				val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
				import spark.implicits._
					
				val df = spark.read.json("file:///C:/data/devices.json")
				println(df.schema)
				  
				 }

}