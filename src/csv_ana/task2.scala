import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object task2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Financial_csv").getOrCreate()

    var pre_fcsv = spark.sql(
      "select Country, Product, Units_Sold, Sales, COGS, Profit, Year, Month_Number as Month from financial "
    )

    // 用于将所有金钱类列转换为string
    def col2Float(S: String) = {
      var S_deal = S.trim() // 去除首尾空格
      if (S_deal.charAt(0).equals('$')){ // 去除"$"符号
        S_deal = S_deal.substring(1, S_deal.length)
      }
      S_deal = S_deal.trim()
      if (S_deal.indexOf("(") != -1){ // 部分数据呈“$(xxx)”格式，需要清洗括号
        S_deal = S_deal.substring(S_deal.indexOf("(")+1, S_deal.indexOf(")"))
      }
      if (S_deal.charAt(0).equals('-')){ // 部分数据呈“$-”格式，需要转为0
        "0.0"
      }
      else{ // 将xxx,xxx.00中的“,”去除
        S_deal.replace(",","").toFloat.formatted("%.2f")
      }
    }

    val c2f = udf(col2Float _) // 注册为UDF函数，方便DF调用
    val float_2f = udf((str: String) => str.toFloat.formatted("%.2f")) // 将过长小数规范到2位

    pre_fcsv = pre_fcsv.withColumn("Sales", c2f(col("Sales")))
      .withColumn("COGS", c2f(col("COGS")))
      .withColumn("Profit", c2f(col("Profit")))

    // 合并年月，使得后续group更方便
    pre_fcsv = pre_fcsv.withColumn("YM", concat_ws("-",col("Year"), col("Month")))
      .drop("Year","Month")

    // 月度销售额top3国家
    val top3_sales_country = pre_fcsv.groupBy("Country", "YM")
      .agg(sum("Sales") as "Sales_c")
    val win1 = Window.partitionBy("YM").orderBy(- col("Sales_c"))
    val c_sql1 = top3_sales_country.withColumn("top", rank() over win1)
      .filter("top <= 3").withColumn("Sales_c", float_2f(col("Sales_c")))
    c_sql1.show()

    // 月度利润率top3国家
    val top3_profitraio_country = pre_fcsv.groupBy("Country", "YM")
      .agg(sum("COGS") as "COGS_c", sum("Profit") as "Profit_c")
      .withColumn("Profit_ratio", col("Profit_c")/col("COGS_c"))
    val win2 = Window.partitionBy("YM").orderBy(- col("Profit_ratio"))
    val c_sql2 = top3_profitraio_country.withColumn("top", rank() over win2)
      .filter("top <= 3").select(col("Country"), col("YM"), float_2f(col("Profit_ratio")), col("top"))//.withColumn("Profit_ratio", float_2f(col("Sales_c")))
    c_sql2.show()

    // 月度畅销top2商品
    val top2_sold_product = pre_fcsv.groupBy("Product", "YM")
      .agg(sum("Units_Sold") as "Units_Sold_p")
    val win3 = Window.partitionBy("YM").orderBy(- col("Units_Sold_p"))
    val p_sql1 = top2_sold_product.withColumn("top", rank() over win3)
      .filter("top <= 2")
    p_sql1.show()

    // 月度利润额最低top2商品
    val top2_sales_loe_product = pre_fcsv.groupBy("Product", "YM")
      .agg(sum("Profit") as "Profit_p")
    val win4 = Window.partitionBy("YM").orderBy(col("Profit_p"))
    val p_sql2 = top2_sales_loe_product.withColumn("top", rank() over win4)
      .filter("top <= 2").withColumn("Profit_p", float_2f(col("Profit_p")))
    p_sql2.show()

    spark.close()

  }

}
