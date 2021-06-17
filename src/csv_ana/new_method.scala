import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object new_method {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local")//启动本地化计算
      .setAppName("new_method")//设置本程序名称
    val sc=new SparkContext(conf)
    val spark = SparkSession.builder().appName("csv_app2").getOrCreate()

    val csv1=spark.read.option("header", "true").option("multiLine", true).csv("data/input.csv")//读取本地文件
    val csv2=spark.read.option("header", "true").option("multiLine", true).csv("data/attribute_mapping.csv")

    val col2 = csv2.columns //获取csv2的列名
    val brand = col2.slice(1, col2.length) //获取所有brand

    var csv2DF =
      brand.foldLeft(csv2)((df,b)=>{
        df.withColumn(b,concat(lit(b+":"),col(b))) // 给每个brand下的question加上其对应的品牌名
      })
        .select(
          // 将所有brand列合并，以“,”作为分割
          col("Attribute"), concat_ws(",", brand.map(name => col(name)): _*).as("Question")
        )
        .select(
          // 将合并的列展开，使得在行上填充
          col("Attribute"), explode(split(col("Question"),",")).as("Question")
        )
        .withColumn("splitcol",split(col("Question"), ":")) //将brand和问题在列上分割，分隔符“:”
        .select(
          col("Attribute"), // 属性
          col("splitcol").getItem(0).as("Brand"), // 品牌名
          col("splitcol").getItem(1).as("Question") // 问题名
        )
        .drop("splitcol")

    // 获取所有Question：collect后每个元素会在一个数组里，所以map(_(0))取出，并tostring将每个元素转成string类型
    var Ques = csv2DF.select("Question").collect.map(_(0).toString)

    var myList1 = Array("EndDate", "ResponseId") // 第一个csv中需要的元素
    var newcol = myList1++Ques // 与所有问题列表合并
    var needdf = csv1.select(newcol.map(col(_)): _*) // 取出列表中列名对应的的所有列

    needdf = needdf.na.fill("null") //将所有空值填上“null”

    // 与上面csv2DF类似：
    var csv1DF=
      Ques.foldLeft(needdf)((df,q)=>{
        df.withColumn(q,concat(lit(q+":"),col(q))) // 注意，如果上一行不填充，则空值的元素会被自动略去。
      })
        .select(
          col("EndDate"), col("ResponseId"), concat_ws(",", Ques.map(name => col(name)): _*).as("Value")
        )
        .select(
          col("EndDate"), col("ResponseId"), explode(split(col("Value"),",")).as("Value")
        )
        .withColumn("splitcol",split(col("Value"), ":"))
        .select(
          col("EndDate"), col("ResponseId"),
          col("splitcol").getItem(0).as("Question"),
          col("splitcol").getItem(1).as("Value"))
        .drop("splitcol")


    // 最终两个DF join即得到最终所需
    var finalDF = csv1DF.join(csv2DF, Seq("Question"), "left_outer").drop("Question").select("ResponseId", "EndDate", "Brand", "Attribute", "Value")

  }
}
