package com.raoqiang.scala


import com.raoqiang.scala.ObjectSet.{MyVar, Nunique}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object  creditCard {

  // 规定 -> credit_card_balance -> 对应文件名 HC1001HCyyyymmdd.csv

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark SQL basic example").getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    // 读取 application_train文件，需要去掉TARGET列
    var cc = spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("/user/spark/data/HC1003HC"+ args(0) +".csv")

    Array("Active", "Approved", "Completed", "Demand", "Refused", "Sent proposal", "Signed").foreach(x =>
      cc = cc
        .withColumn("NAME_CONTRACT_STATUS_"+x, (cc.col("NAME_CONTRACT_STATUS")===x).cast(IntegerType))
    )


    cc = cc.withColumnRenamed("AMT_RECIVABLE", "AMT_RECEIVABLE")

    cc = cc.withColumn("LIMIT_USE", cc.col("AMT_BALANCE") / cc.col("AMT_CREDIT_LIMIT_ACTUAL"))

    cc = cc.withColumn("PAYMENT_DIV_MIN", cc.col("AMT_PAYMENT_CURRENT") / cc.col("AMT_INST_MIN_REGULARITY"))

    cc = cc.withColumn("LATE_PAYMENT", (cc.col("SK_DPD") > 0).cast(IntegerType))

    cc = cc.withColumn("DRAWING_LIMIT_RATIO", cc.col("AMT_DRAWINGS_ATM_CURRENT") / cc.col("AMT_CREDIT_LIMIT_ACTUAL"))

    var cc_min = cc.groupBy("SK_ID_CURR").min("MONTHS_BALANCE", "PAYMENT_DIV_MIN")

    Array("MONTHS_BALANCE", "PAYMENT_DIV_MIN").foreach(
      x =>
        cc_min = cc_min.withColumnRenamed("min("+x+")", "CC_" + x +"_MIN")
    )

    var cc_mean = cc.groupBy("SK_ID_CURR").mean("AMT_INST_MIN_REGULARITY", "AMT_PAYMENT_TOTAL_CURRENT", "AMT_TOTAL_RECEIVABLE", "CNT_DRAWINGS_ATM_CURRENT", "CNT_DRAWINGS_CURRENT", "CNT_DRAWINGS_POS_CURRENT", "SK_DPD", "LIMIT_USE", "PAYMENT_DIV_MIN")

    Array("AMT_INST_MIN_REGULARITY", "AMT_PAYMENT_TOTAL_CURRENT", "AMT_TOTAL_RECEIVABLE", "CNT_DRAWINGS_ATM_CURRENT", "CNT_DRAWINGS_CURRENT", "CNT_DRAWINGS_POS_CURRENT", "SK_DPD", "LIMIT_USE", "PAYMENT_DIV_MIN").foreach(
      x =>
        cc_mean = cc_mean.withColumnRenamed("avg("+x+")", "CC_" + x +"_MEAN")
    )

    var cc_max = cc.groupBy("SK_ID_CURR").max( "AMT_BALANCE", "AMT_CREDIT_LIMIT_ACTUAL", "AMT_DRAWINGS_ATM_CURRENT", "AMT_DRAWINGS_CURRENT", "AMT_DRAWINGS_POS_CURRENT", "AMT_INST_MIN_REGULARITY", "AMT_PAYMENT_TOTAL_CURRENT", "AMT_TOTAL_RECEIVABLE", "CNT_DRAWINGS_ATM_CURRENT", "CNT_DRAWINGS_CURRENT", "SK_DPD", "SK_DPD_DEF", "LIMIT_USE", "LATE_PAYMENT")

    Array("AMT_BALANCE", "AMT_CREDIT_LIMIT_ACTUAL", "AMT_DRAWINGS_ATM_CURRENT", "AMT_DRAWINGS_CURRENT", "AMT_DRAWINGS_POS_CURRENT", "AMT_INST_MIN_REGULARITY", "AMT_PAYMENT_TOTAL_CURRENT", "AMT_TOTAL_RECEIVABLE", "CNT_DRAWINGS_ATM_CURRENT", "CNT_DRAWINGS_CURRENT", "SK_DPD", "SK_DPD_DEF", "LIMIT_USE", "LATE_PAYMENT").foreach(
      x =>
        cc_max = cc_max.withColumnRenamed("max("+x+")", "CC_" + x +"_MAX")
    )

    var cc_sum = cc.groupBy("SK_ID_CURR").sum(  "AMT_DRAWINGS_ATM_CURRENT", "AMT_DRAWINGS_CURRENT", "AMT_DRAWINGS_POS_CURRENT", "AMT_PAYMENT_TOTAL_CURRENT", "CNT_DRAWINGS_ATM_CURRENT", "CNT_DRAWINGS_CURRENT", "SK_DPD", "SK_DPD_DEF", "LATE_PAYMENT")

    Array("AMT_DRAWINGS_ATM_CURRENT", "AMT_DRAWINGS_CURRENT", "AMT_DRAWINGS_POS_CURRENT", "AMT_PAYMENT_TOTAL_CURRENT", "CNT_DRAWINGS_ATM_CURRENT", "CNT_DRAWINGS_CURRENT", "SK_DPD", "SK_DPD_DEF", "LATE_PAYMENT").foreach(
      x =>
        cc_sum = cc_sum.withColumnRenamed("sum("+x+")", "CC_" + x +"_SUM")
    )

    spark.udf.register("myvar", MyVar)

    var cc_var = cc.groupBy("SK_ID_CURR").agg(("AMT_PAYMENT_TOTAL_CURRENT", "myvar")).withColumnRenamed("myvar(AMT_PAYMENT_TOTAL_CURRENT)", "CC_AMT_PAYMENT_TOTAL_CURRENT_VAR")

    var cc_agg = cc_mean.join(cc_max, "SK_ID_CURR").join(cc_min, "SK_ID_CURR").join(cc_sum, "SK_ID_CURR").join(cc_var, "SK_ID_CURR")

    //    cc_agg.show(50)

    cc.groupBy("SK_ID_PREV").max("MONTHS_BALANCE")

    var last_months_df = cc.join(cc.coalesce(1).groupBy("SK_ID_PREV").agg(("MONTHS_BALANCE", "max")), Array("SK_ID_PREV"), "left").where($"MONTHS_BALANCE" === $"max(MONTHS_BALANCE)").drop("max(MONTHS_BALANCE)")

    //    last_months_df.show(500)

    //cc.join(cc_mean.join(cc_max, "SK_ID_CURR").join(cc_min, "SK_ID_CURR").join(cc_sum, "SK_ID_CURR").join(cc_var, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")

    // cc_agg = group_and_merge(last_months_df,cc_agg,'CC_LAST_', {'AMT_BALANCE': ['mean', 'max']})
    last_months_df = last_months_df.groupBy("SK_ID_CURR").agg(("AMT_BALANCE", "mean"),("AMT_BALANCE", "max")).withColumnRenamed("max(AMT_BALANCE)", "CC_LAST_AMT_BALANCE_MAX").withColumnRenamed("avg(AMT_BALANCE)", "CC_LAST_AMT_BALANCE_MEAN")

    cc_agg = cc_agg.join(last_months_df, Array("SK_ID_CURR"), "left").drop("NAME_CONTRACT_STATUS")

    //    cc_agg.show(500)



    spark.udf.register("nunique", Nunique)

    //    cc.join(cc.where($"MONTHS_BALANCE" > -12).groupBy("SK_ID_PREV").agg(("SK_ID_PREV", "count")), Array("SK_ID_PREV"), "left").where()

    val cc_agg12 = cc.where($"MONTHS_BALANCE" > -12).groupBy("SK_ID_PREV").agg(("SK_ID_PREV", "nunique")).where($"nunique(SK_ID_PREV)" === 1).join(cc, Array("SK_ID_PREV"), "left").drop("nunique(SK_ID_PREV)")

    //    cc_agg12.show(500)
    // INS_{}M_


    var cc_agg12_mean = cc_agg12.groupBy("SK_ID_CURR").mean("CNT_DRAWINGS_ATM_CURRENT", "AMT_BALANCE", "LIMIT_USE")

    Array("CNT_DRAWINGS_ATM_CURRENT", "AMT_BALANCE", "LIMIT_USE").foreach(x =>
      cc_agg12_mean = cc_agg12_mean.withColumnRenamed("avg(" + x + ")", "INS_12M_" + x + "_MEAN")
    )

    var cc_agg12_max = cc_agg12.groupBy("SK_ID_CURR").max("SK_DPD", "AMT_BALANCE", "LIMIT_USE")

    Array("SK_DPD", "AMT_BALANCE", "LIMIT_USE").foreach(x =>
      cc_agg12_max = cc_agg12_max.withColumnRenamed("max(" + x + ")", "INS_12M_" + x + "_MAX")
    )

    var cc_agg12_sum = cc_agg12.groupBy("SK_ID_CURR").sum("SK_DPD")

    Array("SK_DPD").foreach(x =>
      cc_agg12_sum = cc_agg12_sum.withColumnRenamed("sum(" + x + ")", "INS_12M_" + x + "_SUM")
    )

    cc_agg = cc_agg.join(cc_agg12_mean.join(cc_agg12_max, "SK_ID_CURR").join(cc_agg12_sum, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")

    val cc_agg24 = cc.where($"MONTHS_BALANCE" > -24).groupBy("SK_ID_PREV").agg(("SK_ID_PREV", "nunique")).where($"nunique(SK_ID_PREV)" === 1).join(cc, Array("SK_ID_PREV"), "left").drop("nunique(SK_ID_PREV)")

    //    cc_agg24.show(50)
    // INS_{}M_


    var cc_agg24_mean = cc_agg24.groupBy("SK_ID_CURR").mean("CNT_DRAWINGS_ATM_CURRENT", "AMT_BALANCE", "LIMIT_USE")

    Array("CNT_DRAWINGS_ATM_CURRENT", "AMT_BALANCE", "LIMIT_USE").foreach(x =>
      cc_agg24_mean = cc_agg24_mean.withColumnRenamed("avg(" + x + ")", "INS_24M_" + x + "_MEAN")
    )

    var cc_agg24_max = cc_agg24.groupBy("SK_ID_CURR").max("SK_DPD", "AMT_BALANCE", "LIMIT_USE")

    Array("SK_DPD", "AMT_BALANCE", "LIMIT_USE").foreach(x =>
      cc_agg24_max = cc_agg24_max.withColumnRenamed("max(" + x + ")", "INS_24M_" + x + "_MAX")
    )

    var cc_agg24_sum = cc_agg24.groupBy("SK_ID_CURR").sum("SK_DPD")

    Array("SK_DPD").foreach(x =>
      cc_agg24_sum = cc_agg24_sum.withColumnRenamed("sum(" + x + ")", "INS_24M_" + x + "_SUM")
    )

    cc_agg = cc_agg.join(cc_agg24_mean.join(cc_agg24_max, "SK_ID_CURR").join(cc_agg24_sum, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")


    val cc_agg48 = cc.where($"MONTHS_BALANCE" > -48).groupBy("SK_ID_PREV").agg(("SK_ID_PREV", "nunique")).where($"nunique(SK_ID_PREV)" === 1).join(cc, Array("SK_ID_PREV"), "left").drop("nunique(SK_ID_PREV)")

    //    cc_agg48.show(50)
    // INS_{}M_


    var cc_agg48_mean = cc_agg48.groupBy("SK_ID_CURR").mean("CNT_DRAWINGS_ATM_CURRENT", "AMT_BALANCE", "LIMIT_USE")

    Array("CNT_DRAWINGS_ATM_CURRENT", "AMT_BALANCE", "LIMIT_USE").foreach(x =>
      cc_agg48_mean = cc_agg48_mean.withColumnRenamed("avg(" + x + ")", "INS_48M_" + x + "_MEAN")
    )

    var cc_agg48_max = cc_agg48.groupBy("SK_ID_CURR").max("SK_DPD", "AMT_BALANCE", "LIMIT_USE")

    Array("SK_DPD", "AMT_BALANCE", "LIMIT_USE").foreach(x =>
      cc_agg48_max = cc_agg48_max.withColumnRenamed("max(" + x + ")", "INS_48M_" + x + "_MAX")
    )

    var cc_agg48_sum = cc_agg48.groupBy("SK_ID_CURR").sum("SK_DPD")

    Array("SK_DPD").foreach(x =>
      cc_agg48_sum = cc_agg48_sum.withColumnRenamed("sum(" + x + ")", "INS_48M_" + x + "_SUM")
    )

    cc_agg = cc_agg.join(cc_agg48_mean.join(cc_agg48_max, "SK_ID_CURR").join(cc_agg48_sum, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")


    cc_agg.repartition(1).write.format("csv").option("header", "false").save("/user/spark/HC1003HC/"+ args(0))

  }


}

