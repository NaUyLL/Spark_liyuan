import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode


object task1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Financial_csv").getOrCreate()

    //读取本地文件
    val f_csv=spark.read.option("header", "true").option("multiLine", true).csv("data/Financial_Sample.csv")

    val fcol = f_csv.columns

    // 用于处理表头，包括去除括号及里面的中文，去除中间的空格转为"_"，去除两端的空格
    def columnName_deal(S: String)={
      var S_deal = S.trim
      val a = S_deal.indexOf("(")
      val b = S_deal.indexOf("（")
      if (a != -1){
        if (b<a && b != -1)
        {
          S_deal = S_deal.substring(0,b).trim
        }
        else{
          S_deal = S_deal.substring(0,a).trim
        }
      }
      else{
        if (b != -1)
        {
          S_deal = S_deal.substring(0,b).trim
        }
        else{
          S_deal = S_deal.trim
        }
      }
      S_deal.replace(" ", "_")
    }

    val str_trim = udf((str: String) => str.trim()) // 用于消除普通数据的前后空格的UDF
    val dd = udf((str: String) => str.trim.replace("/","-")) // 用于将日期xxxx/xx/xx转为xxxx-xx-xx

    var newf_csv = f_csv.toDF(fcol.map(columnName_deal(_)): _*) // 处理表头

    // 处理日期
    newf_csv = newf_csv.withColumn("Date", dd(col("Date")))

    val newfcol = newf_csv.columns // 将所有列都处理一下前后空格问题
    newfcol.map(column =>{
      newf_csv = newf_csv.withColumn(column, str_trim(col(column)))
    })

    newf_csv.write
      .mode(SaveMode.Append)
      .partitionBy("Year", "Month_Number")
      .saveAsTable("financial")

    spark.close()
  }
}
