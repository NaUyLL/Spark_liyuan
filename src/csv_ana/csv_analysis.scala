import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object csv_analysis {
  def main(args: Array[String]): Unit ={
    /**
     * SparkContext 的初始化需要一个SparkConf对象
     * SparkConf包含了Spark集群的配置的各种参数
     */
    val conf=new SparkConf()
      .setMaster("local")//启动本地化计算
      .setAppName("testRdd")//设置本程序名称


    //Spark程序的编写都是从SparkContext开始的
    val sc=new SparkContext(conf)
    val spark = SparkSession.builder().appName("json_app").getOrCreate()
    //以上的语句等价与val sc=new SparkContext("local","testRdd")
      //.option("header", "true") 将第一行设为名称
    val csv1=spark.read.option("header", "true").csv("data/input.csv")//读取本地文件
    val csv2=spark.read.option("header", "true").csv("data/attribute_mapping.csv")
    csv1.show()
    csv2.show()
    val n1 = csv1.columns



  }
}
