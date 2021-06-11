import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object csv_analysis {
  def main(args: Array[String]): Unit ={
    /**
     * SparkContext 的初始化需要一个SparkConf对象
     * SparkConf包含了Spark集群的配置的各种参数
     */
    val conf=new SparkConf()
      .setMaster("local")//启动本地化计算
      .setAppName("csv")//设置本程序名称
    val sc=new SparkContext(conf)
    val spark = SparkSession.builder().appName("csv_app").getOrCreate()


    // 读取csv格式数据
    // header参数，表示将第一行设置为header
    // multiLine参数，某些数据value内有换行符，不设置的话会使得每行成为一个单独数据行
    val csv1=spark.read.option("header", "true").option("multiLine", true).csv("/usr/local/spark/resources/data/input.csv")//读取本地文件
    val csv2=spark.read.option("header", "true").option("multiLine", true).csv("/usr/local/spark/resources/data/attribute_mapping.csv")

    // 提取出brand数组
    val brand = csv2.columns.filter(!_.contains("Attribute"))
    // 也可用 csv2.columns.slice(col1.indexWhere(_=="xx"), col1.length)


    // 按照每一个brand将数据提取出并连接
    var csv2DF = csv2.select("Attribute",brand(0)).toDF("Attribute", "Question").withColumn("Brand", lit(brand(0)))
    for(i<-(1 until brand.length)){
      csv2DF = csv2DF.union(
        csv2.select("Attribute",brand(i))
          .toDF("Attribute", "Question")
          .withColumn("Brand", lit(brand(i)))
      )
    }

    // 提取出所有问题 （884个问题=17*52）
    var Ques = csv2DF.select("Question").collect.map(_(0).toString)
    // 884条数据对应后面过大，可以取少一点
    // Ques = Ques.slice(100,150)

    // 按照每一个问题将数据提取出并连接
    var csv1DF = csv1.select("ResponseId","EndDate",Ques(0)).toDF("ResponseId","EndDate", "Value").withColumn("Question", lit(Ques(0)))
    for(i<-(1 until Ques.length)){
      csv1DF = csv1DF.union(
        csv1.select("ResponseId","EndDate",Ques(i))
          .toDF("ResponseId","EndDate", "Value")
          .withColumn("Question", lit(Ques(i)))
      )
    }

    // 两个表进行join，并丢弃“Question”，重拍顺序
    val finalDF = csv1DF.join(csv2DF, Seq("Question"), "left_outer")
                        .drop("Question")
                        .select("ResponseId", "EndDate", "Brand", "Attribute", "Value")

    // 写入本地
    finalDF.write.option("header", "true").csv("/usr/local/spark/resources/final")

  }
}
