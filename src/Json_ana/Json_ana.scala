import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object Json_ana {
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
    val data=spark.read.option("multiLine", true).option("mode", "PERMISSIVE").json("data/test.json")//读取本地文件
    // data.printSchema()
    val dfDe = data.select(explode(data("policies"))).toDF("policies")
    // dfDe.printSchema()
    val dfDe2 = dfDe.select(dfDe("policies.service"), explode(dfDe("policies.policyItems.users"))).toDF("service","users")
    val dfDe3 = dfDe2.select(dfDe2("service"), explode(dfDe2("users"))).toDF("service","users")
    val rddjson = dfDe3.distinct.rdd
    val servicekey_rdd = rddjson.map(row=>(row.getString(0),row.getString(1)))
    val userkey_rdd = rddjson.map(row=>(row.getString(1),row.getString(0)))

  }
}
