package com.raoqiang.scala

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import com.raoqiang.scala.ObjectSet._

object PosCash {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("POS CASH").getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    // 读取 application_train文件，需要去掉TARGET列

    var pos = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("/user/spark/data/HC1005HC"+ args(0) +".csv")
//    pos.where("SK_ID_CURR=100170").show(500)


    Array("Active", "Amortized debt",
      "Approved", "Canceled", "Completed",
      "Demand", "Returned to the store", "Signed", "XNA").foreach(x =>
      pos = pos
        .withColumn("NAME_CONTRACT_STATUS_"+x, (pos.col("NAME_CONTRACT_STATUS")===x).cast(IntegerType))
    )


    pos = pos.withColumn("LATE_PAYMENT", pos.col("SK_DPD").cast(BooleanType).cast(IntegerType)).drop("NAME_CONTRACT_STATUS")

//    pos.where("SK_ID_CURR=100170").show(500)

    var pos_agg = pos.groupBy("SK_ID_CURR").mean("NAME_CONTRACT_STATUS_Active",
      "NAME_CONTRACT_STATUS_Amortized debt", "NAME_CONTRACT_STATUS_Approved", "NAME_CONTRACT_STATUS_Canceled",
      "NAME_CONTRACT_STATUS_Completed", "NAME_CONTRACT_STATUS_Demand", "NAME_CONTRACT_STATUS_Returned to the store",
      "NAME_CONTRACT_STATUS_Signed", "NAME_CONTRACT_STATUS_XNA", "SK_DPD", "SK_DPD_DEF", "LATE_PAYMENT")
    Array("NAME_CONTRACT_STATUS_Active",
      "NAME_CONTRACT_STATUS_Amortized debt", "NAME_CONTRACT_STATUS_Approved", "NAME_CONTRACT_STATUS_Canceled",
      "NAME_CONTRACT_STATUS_Completed", "NAME_CONTRACT_STATUS_Demand", "NAME_CONTRACT_STATUS_Returned to the store",
      "NAME_CONTRACT_STATUS_Signed", "NAME_CONTRACT_STATUS_XNA", "SK_DPD", "SK_DPD_DEF", "LATE_PAYMENT").foreach(x =>
      pos_agg = pos_agg.withColumnRenamed("avg("+ x +")", "POS_" + x + "_MEAN")
    )

    var pos_agg_min = pos.groupBy("SK_ID_CURR").min("MONTHS_BALANCE").withColumnRenamed("min(MONTHS_BALANCE)", "POS_MONTHS_BALANCE_MIN")

    spark.udf.register("nunique", Nunique)
    spark.udf.register("myVar", MyVar)

    var pos_agg_nunique = pos.groupBy("SK_ID_CURR").agg(Map("SK_ID_PREV" -> "nunique", "MONTHS_BALANCE" -> "count", "SK_DPD"->"myVar")).withColumnRenamed("myVar(SK_DPD)", "POS_SK_DPD_VAR").withColumnRenamed("nunique(SK_ID_PREV)", "POS_SK_ID_PREV_NUNIQUE").withColumnRenamed("count(MONTHS_BALANCE)", "POS_MONTHS_BALANCE_SIZE")


    var pos_agg_max = pos.groupBy("SK_ID_CURR").max("MONTHS_BALANCE", "SK_DPD", "SK_DPD_DEF")
    Array("MONTHS_BALANCE", "SK_DPD", "SK_DPD_DEF").foreach(x =>
      pos_agg_max = pos_agg_max.withColumnRenamed("max("+ x +")", "POS_" + x + "_MAX")
    )

    // "MONTHS_BALANCE", "SK_DPD"

    var pos_agg_sum = pos.groupBy("SK_ID_CURR").sum("SK_DPD_DEF", "SK_DPD")
    Array("SK_DPD_DEF", "SK_DPD").foreach(x =>
      pos_agg_sum = pos_agg_sum.withColumnRenamed("sum("+ x +")", "POS_" + x + "_SUM")
    )

//    var pos_agg_size = pos.groupBy("SK_ID_CURR").agg(Map())

    pos_agg = pos_agg.join(pos_agg_min, "SK_ID_CURR").join(pos_agg_max, "SK_ID_CURR").join(pos_agg_nunique, "SK_ID_CURR").join(pos_agg_sum, "SK_ID_CURR")


    val sort_pos = pos.sort("SK_ID_PREV", "MONTHS_BALANCE")
//    sort_pos.show(500)

    var df = sort_pos.coalesce(1).groupBy("SK_ID_PREV").
      agg(
        ("SK_ID_CURR", "first"), ("CNT_INSTALMENT", "last"), ("CNT_INSTALMENT", "first"),
        ("NAME_CONTRACT_STATUS_Completed", "mean"), ("CNT_INSTALMENT_FUTURE", "last"),
        ("MONTHS_BALANCE", "max")
      )
    df = df.withColumn("POS_COMPLETED_BEFORE_MEAN", df.col("first(CNT_INSTALMENT)") - df.col("last(CNT_INSTALMENT)"))
      .withColumn("POS_REMAINING_INSTALMENTS_RATIO", df.col("last(CNT_INSTALMENT_FUTURE)") / df.col("last(CNT_INSTALMENT)"))
      .withColumnRenamed("first(SK_ID_CURR)", "SK_ID_CURR")
      .withColumnRenamed("max(MONTHS_BALANCE)", "MONTHS_BALANCE_MAX")
      .withColumnRenamed("last(CNT_INSTALMENT_FUTURE)", "POS_REMAINING_INSTALMENTS")
      .withColumnRenamed("avg(NAME_CONTRACT_STATUS_Completed)", "POS_LOAN_COMPLETED_MEAN")
      .drop("first(SK_ID_CURR)", "last(CNT_INSTALMENT)", "first(CNT_INSTALMENT)", "avg(NAME_CONTRACT_STATUS_Completed)", "last(CNT_INSTALMENT_FUTURE)", "max(MONTHS_BALANCE)", "SK_ID_PREV")
    df = df.withColumn("POS_COMPLETED_BEFORE_MEAN", (df.col("POS_LOAN_COMPLETED_MEAN") > 0 && df.col("POS_COMPLETED_BEFORE_MEAN") > 0).cast(IntegerType))


    df = df.groupBy("SK_ID_CURR").sum()
      .withColumnRenamed("sum(POS_LOAN_COMPLETED_MEAN)", "POS_LOAN_COMPLETED_MEAN")
      .withColumnRenamed("sum(POS_REMAINING_INSTALMENTS)", "POS_REMAINING_INSTALMENTS")
      .withColumnRenamed("sum(MONTHS_BALANCE_MAX)", "MONTHS_BALANCE_MAX")
      .withColumnRenamed("sum(POS_COMPLETED_BEFORE_MEAN)", "POS_COMPLETED_BEFORE_MEAN")
      .withColumnRenamed("sum(POS_REMAINING_INSTALMENTS_RATIO)", "POS_REMAINING_INSTALMENTS_RATIO")
      .drop("sum(SK_ID_CURR)")


    pos_agg = pos_agg.join(df, Array("SK_ID_CURR"), "left")


    pos = pos.join(pos.select("SK_ID_PREV", "LATE_PAYMENT").groupBy("SK_ID_PREV")
      .sum("LATE_PAYMENT")
      .withColumnRenamed("sum(LATE_PAYMENT)", "LATE_PAYMENT_SUM"), Array("SK_ID_PREV"), "left")

    val sort_pos_ = pos.join(pos.groupBy("SK_ID_PREV").max("MONTHS_BALANCE"), Array("SK_ID_PREV"), "left")
      .where($"MONTHS_BALANCE"===$"max(MONTHS_BALANCE)").drop("max(MONTHS_BALANCE)").sort("SK_ID_PREV", "MONTHS_BALANCE")

    spark.udf.register("top3", Top3)


    pos_agg = pos_agg.join(sort_pos_.groupBy("SK_ID_CURR").agg(("LATE_PAYMENT_SUM", "top3")).withColumnRenamed("top3(LATE_PAYMENT_SUM)", "LATE_PAYMENT_SUM"), Array("SK_ID_CURR"), "left").drop("POS_NAME_CONTRACT_STATUS_Canceled_MEAN", "POS_NAME_CONTRACT_STATUS_Amortized debt_MEAN", "POS_NAME_CONTRACT_STATUS_XNA_MEAN", "MONTHS_BALANCE_MAX")

    pos_agg.repartition(1).write.format("csv").option("header", "false").save("/user/spark/HC1005HC/"+ args(0))

  }


}
