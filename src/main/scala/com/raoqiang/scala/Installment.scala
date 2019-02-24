package com.raoqiang.scala

import com.raoqiang.scala.ObjectSet.{MyVar, Nunique}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object Installment {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Installment").getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    // 读取 application_train文件，需要去掉TARGET列

    var pay = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("/user/spark/data/HC1004HC"+ args(0) +".csv")
    //    pos.where("SK_ID_CURR=100170").show(500)

    //pay = do_sum(pay, ['SK_ID_PREV', 'NUM_INSTALMENT_NUMBER'], 'AMT_PAYMENT', 'AMT_PAYMENT_GROUPED')

    pay.select("SK_ID_PREV", "NUM_INSTALMENT_NUMBER", "AMT_PAYMENT")
      .groupBy("SK_ID_PREV", "NUM_INSTALMENT_NUMBER").sum("AMT_PAYMENT")
      .withColumnRenamed("sum(AMT_PAYMENT)", "AMT_PAYMENT_GROUPED")
    pay = pay.join(pay.select("SK_ID_PREV", "NUM_INSTALMENT_NUMBER", "AMT_PAYMENT")
      .groupBy("SK_ID_PREV", "NUM_INSTALMENT_NUMBER").sum("AMT_PAYMENT")
      .withColumnRenamed("sum(AMT_PAYMENT)", "AMT_PAYMENT_GROUPED"), Array("SK_ID_PREV", "NUM_INSTALMENT_NUMBER"), "left")

    pay = pay.withColumn("PAYMENT_DIFFERENCE", pay.col("AMT_INSTALMENT") - pay.col("AMT_PAYMENT_GROUPED"))
    pay = pay.withColumn("PAYMENT_RATIO", pay.col("AMT_INSTALMENT") / pay.col("AMT_PAYMENT_GROUPED"))
    pay = pay.withColumn("PAID_OVER_AMOUNT", pay.col("AMT_PAYMENT") / pay.col("AMT_INSTALMENT"))
    pay = pay.withColumn("PAID_OVER", (pay.col("PAID_OVER_AMOUNT")>0).cast(IntegerType))
    pay = pay.withColumn("DPD", (pay.col("DAYS_ENTRY_PAYMENT") > pay.col("DAYS_INSTALMENT")).cast(IntegerType))
    pay = pay.withColumn("DBD", (pay.col("DAYS_INSTALMENT") > pay.col("DAYS_ENTRY_PAYMENT")).cast(IntegerType))
    pay = pay.withColumn("LATE_PAYMENT", (pay.col("DBD") > 0).cast(IntegerType))
    pay = pay.withColumn("INSTALMENT_PAYMENT_RATIO", pay.col("AMT_PAYMENT") / pay.col("AMT_INSTALMENT"))
    pay = pay.withColumn("LATE_PAYMENT_RATIO", pay.col("INSTALMENT_PAYMENT_RATIO") * pay.col("LATE_PAYMENT"))
    pay = pay.withColumn("SIGNIFICANT_LATE_PAYMENT", (pay.col("LATE_PAYMENT_RATIO") > 0.05).cast(IntegerType))
    pay = pay.withColumn("DPD_7", (pay.col("DPD") >= 7).cast(IntegerType))
    pay = pay.withColumn("DPD_15", (pay.col("DPD") >= 15).cast(IntegerType))

    var pay_mean = pay.groupBy("SK_ID_CURR").mean("DAYS_ENTRY_PAYMENT", "AMT_INSTALMENT", "AMT_PAYMENT", "DPD",
      "DBD", "PAYMENT_DIFFERENCE", "PAYMENT_RATIO", "LATE_PAYMENT", "SIGNIFICANT_LATE_PAYMENT", "LATE_PAYMENT_RATIO",
      "DPD_7", "DPD_15", "PAID_OVER")

    Array("DAYS_ENTRY_PAYMENT", "AMT_INSTALMENT", "AMT_PAYMENT", "DPD",
      "DBD", "PAYMENT_DIFFERENCE", "PAYMENT_RATIO", "LATE_PAYMENT", "SIGNIFICANT_LATE_PAYMENT", "LATE_PAYMENT_RATIO",
      "DPD_7", "DPD_15", "PAID_OVER").foreach(
      x => pay_mean = pay_mean.withColumnRenamed("avg(" + x + ")", "INS_" + x + "_MEAN")
    )


    var pay_max = pay.groupBy("SK_ID_CURR").max("DAYS_ENTRY_PAYMENT",
      "AMT_INSTALMENT", "AMT_PAYMENT", "DPD", "DBD")

    Array("DAYS_ENTRY_PAYMENT", "AMT_INSTALMENT", "AMT_PAYMENT", "DPD", "DBD").foreach(
      x => pay_max = pay_max.withColumnRenamed("max(" + x + ")", "INS_" + x + "_MAX")
    )


    var pay_min = pay.groupBy("SK_ID_CURR").min("DAYS_ENTRY_PAYMENT", "AMT_INSTALMENT", "AMT_PAYMENT")

    Array("DAYS_ENTRY_PAYMENT", "AMT_INSTALMENT", "AMT_PAYMENT").foreach(
      x => pay_min = pay_min.withColumnRenamed("min(" + x + ")", "INS_" + x + "_MIN")
    )

    var pay_sum = pay.groupBy("SK_ID_CURR").sum("AMT_INSTALMENT", "AMT_PAYMENT", "LATE_PAYMENT", "SIGNIFICANT_LATE_PAYMENT")

    Array("AMT_INSTALMENT", "AMT_PAYMENT", "LATE_PAYMENT", "SIGNIFICANT_LATE_PAYMENT").foreach(
      x => pay_sum = pay_sum.withColumnRenamed("sum(" + x + ")", "INS_" + x + "_SUM")
    )

    var pay_size = pay.groupBy("SK_ID_CURR").agg(("SK_ID_PREV", "count"))

    Array("SK_ID_PREV").foreach(
      x => pay_size = pay_size.withColumnRenamed("count(" + x + ")", "INS_" + x + "_SIZE")
    )

    spark.udf.register("nunique", Nunique)
    spark.udf.register("myvar", MyVar)

    var pay_nunique = pay.groupBy("SK_ID_CURR").agg(("SK_ID_PREV", "nunique"))

    Array("SK_ID_PREV").foreach(
      x => pay_nunique = pay_nunique.withColumnRenamed("nunique(" + x + ")", "INS_" + x + "_NUNIQUE")
    )

    var pay_var = pay.groupBy("SK_ID_CURR").agg(("DPD", "myvar"), ("DBD", "myvar"))

    Array("DPD", "DBD").foreach(
      x => pay_var = pay_var.withColumnRenamed("myvar(" + x + ")", "INS_" + x + "_VAR")
    )


    var pay_agg = pay_mean.join(pay_max, "SK_ID_CURR").join(pay_var, "SK_ID_CURR").join(pay_min, "SK_ID_CURR").join(pay_sum, "SK_ID_CURR").join(pay_size, "SK_ID_CURR").join(pay_nunique, "SK_ID_CURR")


    var recent_prev_id36 = pay.where($"DAYS_INSTALMENT" >= -30*36).select($"SK_ID_CURR", $"SK_ID_PREV").groupBy("SK_ID_CURR").agg(("SK_ID_PREV", "nunique"))

    recent_prev_id36 = pay.join(recent_prev_id36, "SK_ID_CURR").where($"nunique(SK_ID_PREV)".cast(IntegerType) === 1).drop("nunique(SK_ID_PREV)")

    var recent_prev_id36_mean = recent_prev_id36.groupBy("SK_ID_CURR").mean("DAYS_ENTRY_PAYMENT", "AMT_INSTALMENT", "AMT_PAYMENT", "DPD", "DBD", "PAYMENT_DIFFERENCE", "PAYMENT_RATIO", "LATE_PAYMENT", "SIGNIFICANT_LATE_PAYMENT", "LATE_PAYMENT_RATIO", "DPD_7", "DPD_15")

    Array("DAYS_ENTRY_PAYMENT", "AMT_INSTALMENT", "AMT_PAYMENT", "DPD", "DBD", "PAYMENT_DIFFERENCE", "PAYMENT_RATIO", "LATE_PAYMENT", "SIGNIFICANT_LATE_PAYMENT", "LATE_PAYMENT_RATIO", "DPD_7", "DPD_15").foreach(
      x => recent_prev_id36_mean = recent_prev_id36_mean.withColumnRenamed("avg(" + x + ")", "INS_36M_" + x + "_MEAN")
    )

    var recent_prev_id36_min = recent_prev_id36.groupBy("SK_ID_CURR").min("DAYS_ENTRY_PAYMENT", "AMT_INSTALMENT", "AMT_PAYMENT")

    Array("DAYS_ENTRY_PAYMENT", "AMT_INSTALMENT", "AMT_PAYMENT").foreach(
      x => recent_prev_id36_min = recent_prev_id36_min.withColumnRenamed("min(" + x + ")", "INS_36M_" + x + "_MIN")
    )

    var recent_prev_id36_max = recent_prev_id36.groupBy("SK_ID_CURR").max("DAYS_ENTRY_PAYMENT", "AMT_INSTALMENT", "AMT_PAYMENT", "DPD", "DBD")

    Array("DAYS_ENTRY_PAYMENT", "AMT_INSTALMENT", "AMT_PAYMENT", "DPD", "DBD").foreach(
      x => recent_prev_id36_max = recent_prev_id36_max.withColumnRenamed("max(" + x + ")", "INS_36M_" + x + "_MAX")
    )

    var recent_prev_id36_sum = recent_prev_id36.groupBy("SK_ID_CURR").sum("AMT_INSTALMENT", "AMT_PAYMENT")

    Array("AMT_INSTALMENT", "AMT_PAYMENT").foreach(
      x => recent_prev_id36_sum = recent_prev_id36_sum.withColumnRenamed("sum(" + x + ")", "INS_36M_" + x + "_SUM")
    )

    var recent_prev_id36_size = recent_prev_id36.groupBy("SK_ID_CURR").agg(("SK_ID_PREV", "count"))

    Array("SK_ID_PREV").foreach(
      x => recent_prev_id36_size = recent_prev_id36_size.withColumnRenamed("count(" + x + ")", "INS_36M_" + x + "_SIZE")
    )

    var recent_prev_id36_var = recent_prev_id36.groupBy("SK_ID_CURR").agg(("DPD", "myvar"), ("DBD", "myvar"))

    Array("DPD", "DBD").foreach(
      x => recent_prev_id36_var = recent_prev_id36_var.withColumnRenamed("myvar(" + x + ")", "INS_36M_" + x + "_VAR")
    )

    val recent_prev_id36_agg = recent_prev_id36_max
      .join(recent_prev_id36_mean, "SK_ID_CURR")
      .join(recent_prev_id36_min, "SK_ID_CURR")
      .join(recent_prev_id36_sum, "SK_ID_CURR")
      .join(recent_prev_id36_size, "SK_ID_CURR")
      .join(recent_prev_id36_var, "SK_ID_CURR")


    pay_agg = pay_agg.join(recent_prev_id36_agg, Array("SK_ID_CURR"), "left")

    var recent_prev_id60 = pay.where($"DAYS_INSTALMENT" >= -30*60).select($"SK_ID_CURR", $"SK_ID_PREV").groupBy("SK_ID_CURR").agg(("SK_ID_PREV", "nunique"))

    recent_prev_id60 = pay.join(recent_prev_id60, "SK_ID_CURR").where($"nunique(SK_ID_PREV)".cast(IntegerType) === 1).drop("nunique(SK_ID_PREV)")

    var recent_prev_id60_mean = recent_prev_id60.groupBy("SK_ID_CURR").mean("DAYS_ENTRY_PAYMENT", "AMT_INSTALMENT", "AMT_PAYMENT", "DPD", "DBD", "PAYMENT_DIFFERENCE", "PAYMENT_RATIO", "LATE_PAYMENT", "SIGNIFICANT_LATE_PAYMENT", "LATE_PAYMENT_RATIO", "DPD_7", "DPD_15")

    Array("DAYS_ENTRY_PAYMENT", "AMT_INSTALMENT", "AMT_PAYMENT", "DPD", "DBD", "PAYMENT_DIFFERENCE", "PAYMENT_RATIO", "LATE_PAYMENT", "SIGNIFICANT_LATE_PAYMENT", "LATE_PAYMENT_RATIO", "DPD_7", "DPD_15").foreach(
      x => recent_prev_id60_mean = recent_prev_id60_mean.withColumnRenamed("avg(" + x + ")", "INS_60M_" + x + "_MEAN")
    )

    var recent_prev_id60_min = recent_prev_id60.groupBy("SK_ID_CURR").min("DAYS_ENTRY_PAYMENT", "AMT_INSTALMENT", "AMT_PAYMENT")

    Array("DAYS_ENTRY_PAYMENT", "AMT_INSTALMENT", "AMT_PAYMENT").foreach(
      x => recent_prev_id60_min = recent_prev_id60_min.withColumnRenamed("min(" + x + ")", "INS_60M_" + x + "_MIN")
    )

    var recent_prev_id60_max = recent_prev_id60.groupBy("SK_ID_CURR").max("DAYS_ENTRY_PAYMENT", "AMT_INSTALMENT", "AMT_PAYMENT", "DPD", "DBD")

    Array("DAYS_ENTRY_PAYMENT", "AMT_INSTALMENT", "AMT_PAYMENT", "DPD", "DBD").foreach(
      x => recent_prev_id60_max = recent_prev_id60_max.withColumnRenamed("max(" + x + ")", "INS_60M_" + x + "_MAX")
    )

    var recent_prev_id60_sum = recent_prev_id60.groupBy("SK_ID_CURR").sum("AMT_INSTALMENT", "AMT_PAYMENT")

    Array("AMT_INSTALMENT", "AMT_PAYMENT").foreach(
      x => recent_prev_id60_sum = recent_prev_id60_sum.withColumnRenamed("sum(" + x + ")", "INS_60M_" + x + "_SUM")
    )

    var recent_prev_id60_size = recent_prev_id60.groupBy("SK_ID_CURR").agg(("SK_ID_PREV", "count"))

    Array("SK_ID_PREV").foreach(
      x => recent_prev_id60_size = recent_prev_id60_size.withColumnRenamed("count(" + x + ")", "INS_60M_" + x + "_SIZE")
    )

    var recent_prev_id60_var = recent_prev_id60.groupBy("SK_ID_CURR").agg(("DPD", "myvar"), ("DBD", "myvar"))

    Array("DPD", "DBD").foreach(
      x => recent_prev_id60_var = recent_prev_id60_var.withColumnRenamed("myvar(" + x + ")", "INS_60M_" + x + "_VAR")
    )

    val recent_prev_id60_agg = recent_prev_id60_max
      .join(recent_prev_id60_mean, "SK_ID_CURR")
      .join(recent_prev_id60_min, "SK_ID_CURR")
      .join(recent_prev_id60_sum, "SK_ID_CURR")
      .join(recent_prev_id60_size, "SK_ID_CURR")
      .join(recent_prev_id60_var, "SK_ID_CURR")


    pay_agg = pay_agg.join(recent_prev_id60_agg, Array("SK_ID_CURR"), "left")


    pay_agg.repartition(1).write.format("csv").option("header", "false").save("/user/spark/HC1004HC/"+ args(0))

  }
}
