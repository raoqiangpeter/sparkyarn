package com.raoqiang.scala

import com.raoqiang.scala.ObjectSet.{MyVar, Nunique}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import com.raoqiang.scala.ObjectSet.one_hot

object PreApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Get Previous Applications").getOrCreate()
//    val spark = SparkSession.builder().master("local[2]").appName("Spark SQL basic example").config("spark.executor.memory", "10g").config("spark.driver.memory", "4g").config("spark.storage.memoryFraction", "0.4").getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    // 读取 application_train文件，需要去掉TARGET列
    var prev = spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("/user/spark/data/HC1006HC"+ args(0) +".csv")


    // 读取application_test文件
    var pay = spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("/user/spark/data/HC1004HC"+ args(0) +".csv")

    prev = one_hot("NAME_CONTRACT_STATUS", Array("Approved", "Canceled", "Refused", "Unused offer"), prev)

    prev = one_hot("NAME_CONTRACT_TYPE", Array("Cash loans", "Consumer loans", "Revolving loans", "XNA"), prev)

    prev = one_hot("CHANNEL_TYPE", Array("AP+ (Cash loan)", "Car dealer", "Channel of corporate sales", "Contact center", "Country-wide", "Credit and cash offices", "Regional / Local", "Stone"), prev)

    prev = one_hot("NAME_TYPE_SUITE", Array("Children", "Family", "Group of people", "Other_A", "Other_B", "Spouse, partner", "Unaccompanied"), prev)

    prev = one_hot("NAME_YIELD_GROUP", Array("XNA", "high", "low_action", "low_normal", "middle"), prev)

    prev = one_hot("PRODUCT_COMBINATION", Array("Card Street",
      "Card X-Sell", "Cash", "Cash Street: high",
      "Cash Street: low", "Cash Street: middle",
      "Cash X-Sell: high", "Cash X-Sell: low",
      "Cash X-Sell: middle", "POS household with interest",
      "POS household without interest", "POS industry with interest",
      "POS industry without interest", "POS mobile with interest",
      "POS mobile without interest", "POS other with interest",
      "POS others without interest"), prev)

    prev = one_hot("NAME_PRODUCT_TYPE", Array("XNA", "walk-in", "x-sell"), prev)

    prev = one_hot("NAME_CLIENT_TYPE", Array("New", "Refreshed", "Repeater", "XNA"), prev)
    /*
    prev['APPLICATION_CREDIT_DIFF'] = prev['AMT_APPLICATION'] - prev['AMT_CREDIT']
    prev['APPLICATION_CREDIT_RATIO'] = prev['AMT_APPLICATION'] / prev['AMT_CREDIT']
    prev['CREDIT_TO_ANNUITY_RATIO'] = prev['AMT_CREDIT']/prev['AMT_ANNUITY']
    prev['DOWN_PAYMENT_TO_CREDIT'] = prev['AMT_DOWN_PAYMENT'] / prev['AMT_CREDIT']
    # Interest ratio on previous application (simplified)
    total_payment = prev['AMT_ANNUITY'] * prev['CNT_PAYMENT']
    prev['SIMPLE_INTERESTS'] = (total_payment/prev['AMT_CREDIT'] - 1)/prev['CNT_PAYMENT']
     */

//    prev = prev.join(prev.select($"SK_ID_PREV", $"AMT_APPLICATION" - $"AMT_CREDIT" as "APPLICATION_CREDIT_DIFF", $"AMT_APPLICATION" / $"AMT_CREDIT"  as "APPLICATION_CREDIT_RATIO",
//      $"AMT_CREDIT" / $"AMT_ANNUITY" as "CREDIT_TO_ANNUITY_RATIO", $"AMT_DOWN_PAYMENT" / $"AMT_CREDIT" as "DOWN_PAYMENT_TO_CREDIT",
//      $"AMT_ANNUITY"  / $"AMT_CREDIT" -  $"CNT_PAYMENT" / $"CNT_PAYMENT" / $"CNT_PAYMENT" as "SIMPLE_INTERESTS"), "SK_ID_PREV")

    prev = prev.withColumn("APPLICATION_CREDIT_DIFF", $"AMT_APPLICATION" - $"AMT_CREDIT").withColumn("APPLICATION_CREDIT_RATIO", $"AMT_APPLICATION" / $"AMT_CREDIT").withColumn("CREDIT_TO_ANNUITY_RATIO", $"AMT_CREDIT" / $"AMT_ANNUITY").withColumn("DOWN_PAYMENT_TO_CREDIT", $"AMT_DOWN_PAYMENT" / $"AMT_CREDIT").withColumn("SIMPLE_INTERESTS",  $"AMT_ANNUITY"  / $"AMT_CREDIT" -  $"CNT_PAYMENT" / $"CNT_PAYMENT" / $"CNT_PAYMENT")

//    println(prev.columns.toList)
//    prev.show(50)

//    val approved = prev.where("NAME_CONTRACT_STATUS_Approved=1")
//    val active_df = approved.where("DAYS_LAST_DUE=365243")
//    val tmp = active_df.select("SK_ID_PREV").join(pay, Array("SK_ID_PREV"), "inner")
//    tmp.show(50)
//    println(tmp.count())

    var approved = prev.where("NAME_CONTRACT_STATUS_Approved=1")
    var active_df = approved.where("DAYS_LAST_DUE=365243")

//    val active_pay_agg = active_df.select("SK_ID_PREV").join(pay, Array("SK_ID_PREV"), "inner").groupBy("SK_ID_PREV")
//      .sum("AMT_INSTALMENT", "AMT_PAYMENT")
//      .withColumnRenamed("sum(AMT_INSTALMENT)", "AMT_INSTALMENT")
//      .withColumnRenamed("sum(AMT_PAYMENT)", "AMT_PAYMENT")

//    active_pay_agg.show(50)
    // active_pay_agg('INSTALMENT_PAYMENT_DIFF') = active_pay_agg('AMT_INSTALMENT') - active_pay_agg('AMT_PAYMENT')
//    active_pay_agg.join(active_pay_agg.select($"SK_ID_PREV", $"AMT_INSTALMENT"/ $"AMT_PAYMENT" as "INSTALMENT_PAYMENT_DIFF"))

    active_df = active_df.join(active_df.select("SK_ID_PREV").join(pay, Array("SK_ID_PREV"), "inner").groupBy("SK_ID_PREV").sum("AMT_INSTALMENT", "AMT_PAYMENT").withColumnRenamed("sum(AMT_INSTALMENT)", "AMT_INSTALMENT").withColumnRenamed("sum(AMT_PAYMENT)", "AMT_PAYMENT").select($"SK_ID_PREV", $"AMT_INSTALMENT", $"AMT_PAYMENT",
      $"AMT_INSTALMENT"- $"AMT_PAYMENT" as "INSTALMENT_PAYMENT_DIFF"), Array("SK_ID_PREV"), "left")



//    active_df = active_df.join(active_df.select($"SK_ID_PREV", $"AMT_CREDIT" - $"AMT_PAYMENT" as "REMAINING_DEBT",
//      $"AMT_PAYMENT" / $"AMT_CREDIT" as "REPAYMENT_RATIO"), "SK_ID_PREV")


    active_df = active_df
      .withColumn("REMAINING_DEBT",  $"AMT_CREDIT" - $"AMT_PAYMENT")
      .withColumn("REPAYMENT_RATIO",  $"AMT_PAYMENT" / $"AMT_CREDIT")

//    active_df.persist()

//    active_df.orderBy("SK_ID_PREV").show(50)

    val tmp_max = active_df.orderBy("SK_ID_CURR").groupBy("SK_ID_CURR").max("AMT_ANNUITY",
      "AMT_APPLICATION", "AMT_DOWN_PAYMENT", "DAYS_LAST_DUE_1ST_VERSION", "INSTALMENT_PAYMENT_DIFF", "REMAINING_DEBT").withColumnRenamed("max(AMT_ANNUITY)", "PREV_ACTIVE_"+"AMT_ANNUITY"+"_MAX").withColumnRenamed("max(AMT_APPLICATION)", "PREV_ACTIVE_"+"AMT_APPLICATION"+"_MAX").withColumnRenamed("max(AMT_DOWN_PAYMENT)", "PREV_ACTIVE_"+"AMT_DOWN_PAYMENT"+"_MAX").withColumnRenamed("max(DAYS_LAST_DUE_1ST_VERSION)", "PREV_ACTIVE_"+"DAYS_LAST_DUE_1ST_VERSION"+"_MAX").withColumnRenamed("max(INSTALMENT_PAYMENT_DIFF)", "PREV_ACTIVE_"+"INSTALMENT_PAYMENT_DIFF"+"_MAX").withColumnRenamed("max(REMAINING_DEBT)", "PREV_ACTIVE_"+"REMAINING_DEBT"+"_MAX")

    val tmp_mean = active_df.orderBy("SK_ID_CURR").groupBy("SK_ID_CURR").mean("SIMPLE_INTERESTS",
      "AMT_APPLICATION", "AMT_DOWN_PAYMENT", "DAYS_DECISION","CNT_PAYMENT", "DAYS_LAST_DUE_1ST_VERSION", "INSTALMENT_PAYMENT_DIFF", "REMAINING_DEBT", "REPAYMENT_RATIO").withColumnRenamed("avg(SIMPLE_INTERESTS)", "PREV_ACTIVE_"+"SIMPLE_INTERESTS"+"_MEAN").withColumnRenamed("avg(AMT_APPLICATION)", "PREV_ACTIVE_"+"AMT_APPLICATION"+"_MEAN").withColumnRenamed("avg(AMT_DOWN_PAYMENT)", "PREV_ACTIVE_"+"AMT_DOWN_PAYMENT"+"_MEAN").withColumnRenamed("avg(DAYS_DECISION)", "PREV_ACTIVE_"+"DAYS_DECISION"+"_MEAN").withColumnRenamed("avg(CNT_PAYMENT)", "PREV_ACTIVE_"+"CNT_PAYMENT"+"_MEAN").withColumnRenamed("avg(DAYS_LAST_DUE_1ST_VERSION)", "PREV_ACTIVE_"+"DAYS_LAST_DUE_1ST_VERSION"+"_MEAN").withColumnRenamed("avg(INSTALMENT_PAYMENT_DIFF)", "PREV_ACTIVE_"+"INSTALMENT_PAYMENT_DIFF"+"_MEAN").withColumnRenamed("avg(REMAINING_DEBT)", "PREV_ACTIVE_"+"REMAINING_DEBT"+"_MEAN").withColumnRenamed("avg(REPAYMENT_RATIO)", "PREV_ACTIVE_"+"REPAYMENT_RATIO"+"_MEAN")


    val tmp_sum = active_df.orderBy("SK_ID_CURR").groupBy("SK_ID_CURR").sum("AMT_ANNUITY",
      "AMT_CREDIT", "CNT_PAYMENT", "REMAINING_DEBT", "AMT_PAYMENT").withColumnRenamed("sum(AMT_ANNUITY)", "PREV_ACTIVE_"+"AMT_ANNUITY"+"_SUM").withColumnRenamed("sum(AMT_CREDIT)", "PREV_ACTIVE_"+"AMT_CREDIT"+"_SUM").withColumnRenamed("sum(CNT_PAYMENT)", "PREV_ACTIVE_"+"CNT_PAYMENT"+"_SUM").withColumnRenamed("sum(REMAINING_DEBT)", "PREV_ACTIVE_"+"REMAINING_DEBT"+"_SUM").withColumnRenamed("sum(AMT_PAYMENT)", "PREV_ACTIVE_"+"AMT_PAYMENT"+"_SUM")

    val tmp_min = active_df.orderBy("SK_ID_CURR").groupBy("SK_ID_CURR").min("DAYS_DECISION", "DAYS_LAST_DUE_1ST_VERSION").withColumnRenamed("min(DAYS_DECISION)", "PREV_ACTIVE_"+"DAYS_DECISION"+"_MIN").withColumnRenamed("min(DAYS_LAST_DUE_1ST_VERSION)", "PREV_ACTIVE_"+"DAYS_LAST_DUE_1ST_VERSION"+"_MIN")

    spark.udf.register("nunique", Nunique)

    val tmp_nunique = active_df.orderBy("SK_ID_CURR").groupBy("SK_ID_CURR").agg(Map("SK_ID_PREV"->"nunique")).withColumnRenamed("nunique(SK_ID_PREV)", "PREV_ACTIVE_"+"SK_ID_PREV"+"_NUNIQUE")

    var active_agg_df = tmp_max.join(tmp_mean, "SK_ID_CURR").join(tmp_sum, "SK_ID_CURR").join(tmp_min, "SK_ID_CURR").join(tmp_nunique, "SK_ID_CURR")

    active_agg_df = active_agg_df.join(active_agg_df.select($"SK_ID_CURR", $"PREV_ACTIVE_AMT_PAYMENT_SUM" / $"PREV_ACTIVE_AMT_CREDIT_SUM" as "TOTAL_REPAYMENT_RATIO"), "SK_ID_CURR")

//    active_df.unpersist()
//    active_agg_df.show(50)
//    prev.na.fill(value=365243,cols=Array("DAYS_FIRST_DRAWING", "DAYS_FIRST_DUE", "DAYS_LAST_DUE_1ST_VERSION",
//      "DAYS_LAST_DUE", "DAYS_TERMINATION"))

//    prev = prev.na.fill(value=365243,cols=Array("DAYS_FIRST_DRAWING", "DAYS_FIRST_DUE", "DAYS_LAST_DUE_1ST_VERSION",
//      "DAYS_LAST_DUE", "DAYS_TERMINATION"))  //TODO : 365243 -> nan , not nan -> 365243

    import org.apache.spark.sql.functions._

    prev = prev.withColumn("DAYS_FIRST_DRAWING", regexp_replace($"DAYS_FIRST_DRAWING".cast(IntegerType), "365243", ""))
    prev = prev.withColumn("DAYS_FIRST_DUE", regexp_replace($"DAYS_FIRST_DUE".cast(IntegerType), "365243", ""))
    prev = prev.withColumn("DAYS_LAST_DUE_1ST_VERSION", regexp_replace($"DAYS_LAST_DUE_1ST_VERSION".cast(IntegerType), "365243", ""))
    prev = prev.withColumn("DAYS_LAST_DUE", regexp_replace($"DAYS_LAST_DUE".cast(IntegerType), "365243", ""))
    prev = prev.withColumn("DAYS_TERMINATION", regexp_replace($"DAYS_TERMINATION".cast(IntegerType), "365243", ""))
    prev = prev.withColumn("DAYS_TERMINATION", $"DAYS_TERMINATION".cast(IntegerType))
    prev = prev.withColumn("DAYS_LAST_DUE", $"DAYS_LAST_DUE".cast(IntegerType))
    prev = prev.withColumn("DAYS_LAST_DUE_1ST_VERSION", $"DAYS_LAST_DUE_1ST_VERSION".cast(IntegerType))
    prev = prev.withColumn("DAYS_FIRST_DUE", $"DAYS_FIRST_DUE".cast(IntegerType))
    prev = prev.withColumn("DAYS_FIRST_DRAWING", $"DAYS_FIRST_DRAWING".cast(IntegerType))



    prev = prev.withColumn("DAYS_LAST_DUE_DIFF", prev.col("DAYS_LAST_DUE_1ST_VERSION") - prev.col("DAYS_LAST_DUE"))

    approved = approved.withColumn("DAYS_LAST_DUE_DIFF", approved.col("DAYS_LAST_DUE_1ST_VERSION") - approved.col("DAYS_LAST_DUE"))

//    prev.persist()
//    prev.show(50)
//    approved.show(50)

    val prev_mean_agg = Array("NAME_CONTRACT_STATUS_Approved", "NAME_CONTRACT_STATUS_Canceled", "NAME_CONTRACT_STATUS_Refused",
      "NAME_CONTRACT_STATUS_Unused offer", "NAME_CONTRACT_TYPE_Cash loans", "NAME_CONTRACT_TYPE_Consumer loans",
      "NAME_CONTRACT_TYPE_Revolving loans", "NAME_CONTRACT_TYPE_XNA", "CHANNEL_TYPE_AP+ (Cash loan)",
      "CHANNEL_TYPE_Car dealer", "CHANNEL_TYPE_Channel of corporate sales", "CHANNEL_TYPE_Contact center",
      "CHANNEL_TYPE_Country-wide", "CHANNEL_TYPE_Credit and cash offices", "CHANNEL_TYPE_Regional / Local",
      "CHANNEL_TYPE_Stone", "NAME_TYPE_SUITE_Children", "NAME_TYPE_SUITE_Family", "NAME_TYPE_SUITE_Group of people",
      "NAME_TYPE_SUITE_Other_A", "NAME_TYPE_SUITE_Other_B", "NAME_TYPE_SUITE_Spouse, partner", "NAME_TYPE_SUITE_Unaccompanied",
      "NAME_YIELD_GROUP_XNA", "NAME_YIELD_GROUP_high", "NAME_YIELD_GROUP_low_action", "NAME_YIELD_GROUP_low_normal",
      "NAME_YIELD_GROUP_middle", "PRODUCT_COMBINATION_Card Street", "PRODUCT_COMBINATION_Card X-Sell", "PRODUCT_COMBINATION_Cash",
      "PRODUCT_COMBINATION_Cash Street: high", "PRODUCT_COMBINATION_Cash Street: low", "PRODUCT_COMBINATION_Cash Street: middle",
      "PRODUCT_COMBINATION_Cash X-Sell: high", "PRODUCT_COMBINATION_Cash X-Sell: low", "PRODUCT_COMBINATION_Cash X-Sell: middle",
      "PRODUCT_COMBINATION_POS household with interest", "PRODUCT_COMBINATION_POS household without interest",
      "PRODUCT_COMBINATION_POS industry with interest", "PRODUCT_COMBINATION_POS industry without interest",
      "PRODUCT_COMBINATION_POS mobile with interest", "PRODUCT_COMBINATION_POS mobile without interest",
      "PRODUCT_COMBINATION_POS other with interest", "PRODUCT_COMBINATION_POS others without interest",
      "NAME_PRODUCT_TYPE_XNA", "NAME_PRODUCT_TYPE_walk-in", "NAME_PRODUCT_TYPE_x-sell", "NAME_CLIENT_TYPE_New",
      "NAME_CLIENT_TYPE_Refreshed", "NAME_CLIENT_TYPE_Repeater", "NAME_CLIENT_TYPE_XNA", "AMT_ANNUITY",
      "AMT_DOWN_PAYMENT", "HOUR_APPR_PROCESS_START", "RATE_DOWN_PAYMENT", "DAYS_DECISION", "CNT_PAYMENT",
      "CREDIT_TO_ANNUITY_RATIO", "APPLICATION_CREDIT_DIFF", "APPLICATION_CREDIT_RATIO", "DOWN_PAYMENT_TO_CREDIT")

    var prevMean = prev.groupBy("SK_ID_CURR").mean("NAME_CONTRACT_STATUS_Approved", "NAME_CONTRACT_STATUS_Canceled", "NAME_CONTRACT_STATUS_Refused",
      "NAME_CONTRACT_STATUS_Unused offer", "NAME_CONTRACT_TYPE_Cash loans", "NAME_CONTRACT_TYPE_Consumer loans",
      "NAME_CONTRACT_TYPE_Revolving loans", "NAME_CONTRACT_TYPE_XNA", "CHANNEL_TYPE_AP+ (Cash loan)",
      "CHANNEL_TYPE_Car dealer", "CHANNEL_TYPE_Channel of corporate sales", "CHANNEL_TYPE_Contact center",
      "CHANNEL_TYPE_Country-wide", "CHANNEL_TYPE_Credit and cash offices", "CHANNEL_TYPE_Regional / Local",
      "CHANNEL_TYPE_Stone", "NAME_TYPE_SUITE_Children", "NAME_TYPE_SUITE_Family", "NAME_TYPE_SUITE_Group of people",
      "NAME_TYPE_SUITE_Other_A", "NAME_TYPE_SUITE_Other_B", "NAME_TYPE_SUITE_Spouse, partner", "NAME_TYPE_SUITE_Unaccompanied",
      "NAME_YIELD_GROUP_XNA", "NAME_YIELD_GROUP_high", "NAME_YIELD_GROUP_low_action", "NAME_YIELD_GROUP_low_normal",
      "NAME_YIELD_GROUP_middle", "PRODUCT_COMBINATION_Card Street", "PRODUCT_COMBINATION_Card X-Sell", "PRODUCT_COMBINATION_Cash",
      "PRODUCT_COMBINATION_Cash Street: high", "PRODUCT_COMBINATION_Cash Street: low", "PRODUCT_COMBINATION_Cash Street: middle",
      "PRODUCT_COMBINATION_Cash X-Sell: high", "PRODUCT_COMBINATION_Cash X-Sell: low", "PRODUCT_COMBINATION_Cash X-Sell: middle",
      "PRODUCT_COMBINATION_POS household with interest", "PRODUCT_COMBINATION_POS household without interest",
      "PRODUCT_COMBINATION_POS industry with interest", "PRODUCT_COMBINATION_POS industry without interest",
      "PRODUCT_COMBINATION_POS mobile with interest", "PRODUCT_COMBINATION_POS mobile without interest",
      "PRODUCT_COMBINATION_POS other with interest", "PRODUCT_COMBINATION_POS others without interest",
      "NAME_PRODUCT_TYPE_XNA", "NAME_PRODUCT_TYPE_walk-in", "NAME_PRODUCT_TYPE_x-sell", "NAME_CLIENT_TYPE_New",
      "NAME_CLIENT_TYPE_Refreshed", "NAME_CLIENT_TYPE_Repeater", "NAME_CLIENT_TYPE_XNA", "AMT_ANNUITY",
      "AMT_DOWN_PAYMENT", "HOUR_APPR_PROCESS_START", "RATE_DOWN_PAYMENT", "DAYS_DECISION", "CNT_PAYMENT",
      "CREDIT_TO_ANNUITY_RATIO", "APPLICATION_CREDIT_DIFF", "APPLICATION_CREDIT_RATIO", "DOWN_PAYMENT_TO_CREDIT")
    prev_mean_agg.foreach(x =>
      prevMean = prevMean.withColumnRenamed("avg("+x+")", "PREV_"+ x + "_MEAN")
    )


    val prev_min_agg = Array("AMT_ANNUITY", "HOUR_APPR_PROCESS_START", "DAYS_DECISION", "APPLICATION_CREDIT_DIFF", "APPLICATION_CREDIT_RATIO")

    var prevMin = prev.groupBy("SK_ID_CURR").min("AMT_ANNUITY", "HOUR_APPR_PROCESS_START", "DAYS_DECISION", "APPLICATION_CREDIT_DIFF", "APPLICATION_CREDIT_RATIO")
    prev_min_agg.foreach(x =>
      prevMin = prevMin.withColumnRenamed("min("+x+")", "PREV_"+ x + "_MIN")
    )

    val prev_max_agg = Array("AMT_ANNUITY", "AMT_DOWN_PAYMENT", "HOUR_APPR_PROCESS_START", "RATE_DOWN_PAYMENT", "DAYS_DECISION", "CNT_PAYMENT", "DAYS_TERMINATION", "CREDIT_TO_ANNUITY_RATIO", "APPLICATION_CREDIT_DIFF", "APPLICATION_CREDIT_RATIO")

    var prevMax = prev.groupBy("SK_ID_CURR").max("AMT_ANNUITY", "AMT_DOWN_PAYMENT", "HOUR_APPR_PROCESS_START", "RATE_DOWN_PAYMENT", "DAYS_DECISION", "CNT_PAYMENT", "DAYS_TERMINATION", "CREDIT_TO_ANNUITY_RATIO", "APPLICATION_CREDIT_DIFF", "APPLICATION_CREDIT_RATIO")
    prev_max_agg.foreach(x =>
      prevMax = prevMax.withColumnRenamed("max("+x+")", "PREV_"+ x + "_MAX")
    )

    val prev_var_agg = Array("APPLICATION_CREDIT_RATIO")
    spark.udf.register("myVar", MyVar)
    var prevVar = prev.groupBy("SK_ID_CURR").agg(Map("APPLICATION_CREDIT_RATIO" -> "myVar"))
    prev_var_agg.foreach(x =>
      prevVar = prevVar.withColumnRenamed("myVar("+x+")", "PREV_"+ x + "_VAR")
    )

    val prev_nunique_agg = Array("SK_ID_PREV")


    var prevNunique = prev.groupBy("SK_ID_CURR").agg(Map("SK_ID_PREV"->"nunique"))
    prev_nunique_agg.foreach(x =>
      prevNunique = prevNunique.withColumnRenamed("nunique("+x+")", "PREV_"+ x + "_NUNIQUE")
    )

    var agg_prev = prevMax.join(prevMean, "SK_ID_CURR").join(prevMin, "SK_ID_CURR").join(prevVar, "SK_ID_CURR").join(prevNunique, "SK_ID_CURR").join(active_agg_df, Array("SK_ID_CURR"), "left")


    // 处理approved
    val approved_mean_agg = Array("AMT_ANNUITY", "AMT_CREDIT", "DAYS_DECISION", "CNT_PAYMENT", "DAYS_TERMINATION",
      "CREDIT_TO_ANNUITY_RATIO", "APPLICATION_CREDIT_RATIO", "DAYS_FIRST_DRAWING", "DAYS_FIRST_DUE",
      "DAYS_LAST_DUE_1ST_VERSION", "DAYS_LAST_DUE", "DAYS_LAST_DUE_DIFF", "SIMPLE_INTERESTS")

    var approvedMean = approved.groupBy("SK_ID_CURR").mean("AMT_ANNUITY", "AMT_CREDIT", "DAYS_DECISION", "CNT_PAYMENT", "DAYS_TERMINATION",
      "CREDIT_TO_ANNUITY_RATIO", "APPLICATION_CREDIT_RATIO", "DAYS_FIRST_DRAWING", "DAYS_FIRST_DUE",
      "DAYS_LAST_DUE_1ST_VERSION", "DAYS_LAST_DUE", "DAYS_LAST_DUE_DIFF", "SIMPLE_INTERESTS")
    approved_mean_agg.foreach(x =>
      approvedMean = approvedMean.withColumnRenamed("avg("+x+")", "APPROVED_"+ x + "_MEAN")
    )


    val approved_max_agg = Array("AMT_ANNUITY", "AMT_CREDIT", "AMT_DOWN_PAYMENT", "AMT_GOODS_PRICE",
      "HOUR_APPR_PROCESS_START", "CNT_PAYMENT", "CREDIT_TO_ANNUITY_RATIO", "APPLICATION_CREDIT_DIFF",
      "APPLICATION_CREDIT_RATIO", "DAYS_FIRST_DRAWING", "DAYS_LAST_DUE_1ST_VERSION", "DAYS_LAST_DUE",
      "DAYS_LAST_DUE_DIFF", "SIMPLE_INTERESTS")

    var approvedMax = approved.groupBy("SK_ID_CURR").max("AMT_ANNUITY", "AMT_CREDIT", "AMT_DOWN_PAYMENT", "AMT_GOODS_PRICE",
      "HOUR_APPR_PROCESS_START", "CNT_PAYMENT", "CREDIT_TO_ANNUITY_RATIO", "APPLICATION_CREDIT_DIFF",
      "APPLICATION_CREDIT_RATIO", "DAYS_FIRST_DRAWING", "DAYS_LAST_DUE_1ST_VERSION", "DAYS_LAST_DUE",
      "DAYS_LAST_DUE_DIFF", "SIMPLE_INTERESTS")
    approved_max_agg.foreach(x =>
      approvedMax = approvedMax.withColumnRenamed("max("+x+")", "APPROVED_"+ x + "_MAX")
    )

    val approved_min_agg = Array("AMT_ANNUITY", "AMT_CREDIT", "HOUR_APPR_PROCESS_START", "DAYS_DECISION",
      "APPLICATION_CREDIT_RATIO", "DAYS_FIRST_DUE", "DAYS_LAST_DUE_1ST_VERSION", "DAYS_LAST_DUE_DIFF", "SIMPLE_INTERESTS")

    var approvedMin = approved.groupBy("SK_ID_CURR").min("AMT_ANNUITY", "AMT_CREDIT", "HOUR_APPR_PROCESS_START", "DAYS_DECISION",
      "APPLICATION_CREDIT_RATIO", "DAYS_FIRST_DUE", "DAYS_LAST_DUE_1ST_VERSION", "DAYS_LAST_DUE_DIFF", "SIMPLE_INTERESTS")
    approved_min_agg.foreach(x =>
      approvedMin = approvedMin.withColumnRenamed("min("+x+")", "APPROVED_"+ x + "_MIN")
    )

    val approved_nunique_agg = Array("SK_ID_PREV")


    var approvedNunique = approved.groupBy("SK_ID_CURR").agg(Map("SK_ID_PREV"->"nunique"))
    approved_nunique_agg.foreach(x =>
      approvedNunique = approvedNunique.withColumnRenamed("nunique("+x+")", "APPROVED_"+ x + "_NUNIQUE")
    )

    agg_prev = agg_prev.join(approvedMax.join(approvedMean, "SK_ID_CURR").join(approvedMin, "SK_ID_CURR").join(approvedNunique, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")


    val refused = prev.where("NAME_CONTRACT_STATUS_Refused=1")

//    refused.persist()

    val refused_min_agg = Array("AMT_CREDIT", "DAYS_DECISION",
      "APPLICATION_CREDIT_DIFF", "APPLICATION_CREDIT_RATIO")

    var refusedMin = refused.groupBy("SK_ID_CURR").min("AMT_CREDIT", "DAYS_DECISION",
      "APPLICATION_CREDIT_DIFF", "APPLICATION_CREDIT_RATIO")
    refused_min_agg.foreach(x =>
      refusedMin = refusedMin.withColumnRenamed("min("+x+")", "REFUSED_"+ x + "_MIN")
    )

    val refused_max_agg = Array("AMT_APPLICATION", "AMT_CREDIT", "DAYS_DECISION", "CNT_PAYMENT", "APPLICATION_CREDIT_DIFF")

    var refusedMax = refused.groupBy("SK_ID_CURR").max("AMT_APPLICATION", "AMT_CREDIT", "DAYS_DECISION", "CNT_PAYMENT", "APPLICATION_CREDIT_DIFF")
    refused_max_agg.foreach(x =>
      refusedMax = refusedMax.withColumnRenamed("max("+x+")", "REFUSED_"+ x + "_MAX")
    )

    val refused_mean_agg = Array("AMT_APPLICATION", "DAYS_DECISION", "CNT_PAYMENT", "APPLICATION_CREDIT_DIFF", "APPLICATION_CREDIT_RATIO", "NAME_CONTRACT_TYPE_Consumer loans", "NAME_CONTRACT_TYPE_Cash loans", "NAME_CONTRACT_TYPE_Revolving loans")

    var refusedMean = refused.groupBy("SK_ID_CURR").mean("AMT_APPLICATION", "DAYS_DECISION", "CNT_PAYMENT", "APPLICATION_CREDIT_DIFF", "APPLICATION_CREDIT_RATIO", "NAME_CONTRACT_TYPE_Consumer loans", "NAME_CONTRACT_TYPE_Cash loans", "NAME_CONTRACT_TYPE_Revolving loans")
    refused_mean_agg.foreach(x =>
      refusedMean = refusedMean.withColumnRenamed("avg("+x+")", "REFUSED_"+ x + "_MEAN")
    )

    val refused_var_agg = Array("APPLICATION_CREDIT_DIFF")
    var refusedVar = refused.groupBy("SK_ID_CURR").agg(Map("APPLICATION_CREDIT_DIFF" -> "myVar"))
    refused_var_agg.foreach(x =>
      refusedVar = refusedVar.withColumnRenamed("myVar("+x+")", "REFUSED_"+ x + "_VAR")
    )


    agg_prev = agg_prev.join(refusedVar.join(refusedMax, "SK_ID_CURR").join(refusedMin, "SK_ID_CURR").join(refusedMean, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")

//    agg_prev.show(50)
//    refused.unpersist()

//    prev.select("NAME_CONTRACT_TYPE_Consumer loans").show(50)

    val consumer_type_df = prev.where($"NAME_CONTRACT_TYPE_Consumer loans"===1)

//    consumer_type_df.show(50)

    var consumer_type_df_max = consumer_type_df.groupBy("SK_ID_CURR").max("AMT_ANNUITY", "SIMPLE_INTERESTS",
      "APPLICATION_CREDIT_RATIO", "DAYS_DECISION", "DAYS_LAST_DUE_1ST_VERSION")
    Array("AMT_ANNUITY", "SIMPLE_INTERESTS",
      "APPLICATION_CREDIT_RATIO", "DAYS_DECISION", "DAYS_LAST_DUE_1ST_VERSION").foreach( x =>
      consumer_type_df_max = consumer_type_df_max.withColumnRenamed("max("+x+")", "PREV_Consumer_"+x+"_MAX")
    )

    var consumer_type_df_mean = consumer_type_df.groupBy("SK_ID_CURR").mean("AMT_ANNUITY", "SIMPLE_INTERESTS",
      "APPLICATION_CREDIT_RATIO", "DAYS_LAST_DUE_1ST_VERSION", "CNT_PAYMENT")
    Array("AMT_ANNUITY", "SIMPLE_INTERESTS",
      "APPLICATION_CREDIT_RATIO", "DAYS_LAST_DUE_1ST_VERSION", "CNT_PAYMENT").foreach( x =>
      consumer_type_df_mean = consumer_type_df_mean.withColumnRenamed("avg("+ x +")", "PREV_Consumer_" + x + "_MEAN")
    )

    var consumer_type_df_min = consumer_type_df.groupBy("SK_ID_CURR").min("SIMPLE_INTERESTS", "APPLICATION_CREDIT_DIFF",
      "APPLICATION_CREDIT_RATIO")
    Array("SIMPLE_INTERESTS", "APPLICATION_CREDIT_DIFF",
      "APPLICATION_CREDIT_RATIO").foreach( x =>
      consumer_type_df_min = consumer_type_df_min.withColumnRenamed("min(" + x + ")", "PREV_Consumer_" + x + "_MIN")
    )

    var consumer_type_df_var = consumer_type_df.groupBy("SK_ID_CURR").agg(Map("SIMPLE_INTERESTS" -> "myVar", "APPLICATION_CREDIT_DIFF"->"myVar"))
    Array("SIMPLE_INTERESTS", "APPLICATION_CREDIT_DIFF").foreach( x =>
      consumer_type_df_var = consumer_type_df_var.withColumnRenamed("myVar(" + x + ")", "PREV_Consumer_" + x + "_VAR")
    )

    var consumer_type_df_sum = consumer_type_df.groupBy("SK_ID_CURR").sum("AMT_CREDIT")
    Array("AMT_CREDIT").foreach( x =>
      consumer_type_df_sum = consumer_type_df_sum.withColumnRenamed("sum(" + x + ")", "PREV_Consumer_" + x + "_SUM")
    )

    agg_prev = agg_prev.join(consumer_type_df_var.join(consumer_type_df_max, "SK_ID_CURR").
      join(consumer_type_df_min, "SK_ID_CURR").join(consumer_type_df_sum, "SK_ID_CURR").
      join(consumer_type_df_mean, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")




    val cash_type_df = prev.where($"NAME_CONTRACT_TYPE_Cash loans"===1)

    var cash_type_df_max = cash_type_df.groupBy("SK_ID_CURR").max("AMT_ANNUITY", "SIMPLE_INTERESTS",
      "APPLICATION_CREDIT_RATIO", "DAYS_DECISION", "DAYS_LAST_DUE_1ST_VERSION")
    Array("AMT_ANNUITY", "SIMPLE_INTERESTS",
      "APPLICATION_CREDIT_RATIO", "DAYS_DECISION", "DAYS_LAST_DUE_1ST_VERSION").foreach( x =>
      cash_type_df_max = cash_type_df_max.withColumnRenamed("max("+x+")", "PREV_Cash_"+x+"_MAX")
    )

    var cash_type_df_mean = cash_type_df.groupBy("SK_ID_CURR").mean("AMT_ANNUITY", "SIMPLE_INTERESTS",
      "APPLICATION_CREDIT_RATIO", "DAYS_LAST_DUE_1ST_VERSION", "CNT_PAYMENT")
    Array("AMT_ANNUITY", "SIMPLE_INTERESTS",
      "APPLICATION_CREDIT_RATIO", "DAYS_LAST_DUE_1ST_VERSION", "CNT_PAYMENT").foreach( x =>
      cash_type_df_mean = cash_type_df_mean.withColumnRenamed("avg("+ x +")", "PREV_Cash_" + x + "_MEAN")
    )

    var cash_type_df_min = cash_type_df.groupBy("SK_ID_CURR").min("SIMPLE_INTERESTS", "APPLICATION_CREDIT_DIFF",
      "APPLICATION_CREDIT_RATIO")
    Array("SIMPLE_INTERESTS", "APPLICATION_CREDIT_DIFF",
      "APPLICATION_CREDIT_RATIO").foreach( x =>
      cash_type_df_min = cash_type_df_min.withColumnRenamed("min(" + x + ")", "PREV_Cash_" + x + "_MIN")
    )

    var cash_type_df_var = cash_type_df.groupBy("SK_ID_CURR").agg(Map("SIMPLE_INTERESTS" -> "myVar", "APPLICATION_CREDIT_DIFF"->"myVar"))
    Array("SIMPLE_INTERESTS", "APPLICATION_CREDIT_DIFF").foreach( x =>
      cash_type_df_var = cash_type_df_var.withColumnRenamed("myVar(" + x + ")", "PREV_Cash_" + x + "_VAR")
    )

    var cash_type_df_sum = cash_type_df.groupBy("SK_ID_CURR").sum("AMT_CREDIT")
    Array("AMT_CREDIT").foreach( x =>
      cash_type_df_sum = cash_type_df_sum.withColumnRenamed("sum(" + x + ")", "PREV_Cash_" + x + "_SUM")
    )

    agg_prev = agg_prev.join(cash_type_df_var.join(cash_type_df_max, "SK_ID_CURR").join(cash_type_df_min, "SK_ID_CURR").join(cash_type_df_sum, "SK_ID_CURR").join(cash_type_df_mean, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")




    pay = pay.withColumn("LATE_PAYMENT", pay.col("DAYS_ENTRY_PAYMENT") > pay.col("DAYS_INSTALMENT"))
    pay = pay.withColumn("LATE_PAYMENT", pay.col("LATE_PAYMENT").cast(IntegerType))

//    pay.select("LATE_PAYMENT").show(50)
    val pay_to_agg = pay.where("LATE_PAYMENT > 0").select("SK_ID_PREV").distinct().join(prev, Array("SK_ID_PREV"), "left")

//    pay_to_agg.persist()



    var pay_to_agg_mean = pay_to_agg.groupBy("SK_ID_CURR").mean("DAYS_DECISION",
      "DAYS_LAST_DUE_1ST_VERSION", "NAME_CONTRACT_TYPE_Consumer loans", "NAME_CONTRACT_TYPE_Cash loans",
      "NAME_CONTRACT_TYPE_Revolving loans")

    Array("DAYS_DECISION", "DAYS_LAST_DUE_1ST_VERSION", "NAME_CONTRACT_TYPE_Consumer loans",
      "NAME_CONTRACT_TYPE_Cash loans", "NAME_CONTRACT_TYPE_Revolving loans").foreach(x =>
      pay_to_agg_mean = pay_to_agg_mean.withColumnRenamed("avg("+ x +")", "PREV_LATE_" + x + "_MEAN")
    )

    var pay_to_agg_max = pay_to_agg.groupBy("SK_ID_CURR").max("DAYS_DECISION", "DAYS_LAST_DUE_1ST_VERSION")

    Array("DAYS_DECISION", "DAYS_LAST_DUE_1ST_VERSION").foreach(x =>
      pay_to_agg_max = pay_to_agg_max.withColumnRenamed("max("+ x +")", "PREV_LATE_" + x + "_MAX")
    )

    var pay_to_agg_min = pay_to_agg.groupBy("SK_ID_CURR").min("DAYS_DECISION", "DAYS_LAST_DUE_1ST_VERSION", "APPLICATION_CREDIT_DIFF")

    Array("DAYS_DECISION", "DAYS_LAST_DUE_1ST_VERSION", "APPLICATION_CREDIT_DIFF").foreach(x =>
      pay_to_agg_min = pay_to_agg_min.withColumnRenamed("min("+ x +")", "PREV_LATE_" + x + "_MIN")
    )

//    pay_to_agg_mean.join(pay_to_agg_min, "SK_ID_CURR").join(pay_to_agg_max, "SK_ID_CURR").show(50)

    agg_prev = agg_prev.join(pay_to_agg_mean.join(pay_to_agg_min, "SK_ID_CURR").join(pay_to_agg_max, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")
//    pay.where("DAYS_ENTRY_PAYMENT > DAYS_INSTALMENT").
//      select("SK_ID_PREV").distinct().join(prev, Array("SK_ID_PREV"), "left")

    // pay['LATE_PAYMENT'] = pay['LATE_PAYMENT'].apply(lambda x: 1 if x > 0 else 0)

//    pay.withColumn("")
//    pay_to_agg.unpersist()



    val time_frame_df12 = prev.where("DAYS_DECISION >= -360")

    var time_frame_df12_mean = time_frame_df12.groupBy("SK_ID_CURR").mean("AMT_ANNUITY",
      "SIMPLE_INTERESTS", "DAYS_DECISION", "DAYS_LAST_DUE_1ST_VERSION", "APPLICATION_CREDIT_RATIO",
      "NAME_CONTRACT_TYPE_Consumer loans", "NAME_CONTRACT_TYPE_Cash loans", "NAME_CONTRACT_TYPE_Revolving loans")
    Array("AMT_ANNUITY", "SIMPLE_INTERESTS", "DAYS_DECISION", "DAYS_LAST_DUE_1ST_VERSION", "APPLICATION_CREDIT_RATIO",
      "NAME_CONTRACT_TYPE_Consumer loans", "NAME_CONTRACT_TYPE_Cash loans", "NAME_CONTRACT_TYPE_Revolving loans").foreach( x =>
      time_frame_df12_mean = time_frame_df12_mean.withColumnRenamed("avg("+ x +")", "PREV_LAST12M_" + x + "_MEAN")
    )

    var time_frame_df12_min = time_frame_df12.groupBy("SK_ID_CURR").min("DAYS_DECISION", "DAYS_LAST_DUE_1ST_VERSION", "APPLICATION_CREDIT_DIFF", "APPLICATION_CREDIT_RATIO")
    Array("DAYS_DECISION", "DAYS_LAST_DUE_1ST_VERSION", "APPLICATION_CREDIT_DIFF", "APPLICATION_CREDIT_RATIO").foreach( x =>
      time_frame_df12_min = time_frame_df12_min.withColumnRenamed("min("+ x +")", "PREV_LAST12M_" + x + "_MIN")
    )

    var time_frame_df12_max = time_frame_df12.groupBy("SK_ID_CURR").max("AMT_ANNUITY", "SIMPLE_INTERESTS", "DAYS_LAST_DUE_1ST_VERSION", "APPLICATION_CREDIT_RATIO")
    Array("AMT_ANNUITY", "SIMPLE_INTERESTS", "DAYS_LAST_DUE_1ST_VERSION", "APPLICATION_CREDIT_RATIO").foreach( x =>
      time_frame_df12_max = time_frame_df12_max.withColumnRenamed("max("+ x +")", "PREV_LAST12M_" + x + "_MAX")
    )

    var time_frame_df12_sum = time_frame_df12.groupBy("SK_ID_CURR").sum("AMT_CREDIT")
    Array("AMT_CREDIT").foreach( x =>
      time_frame_df12_sum = time_frame_df12_sum.withColumnRenamed("sum("+ x +")", "PREV_LAST12M_" + x + "_SUM")
    )

    agg_prev = agg_prev.join(time_frame_df12_mean.
      join(time_frame_df12_min, "SK_ID_CURR").join(time_frame_df12_max, "SK_ID_CURR").
      join(time_frame_df12_sum, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")

//    time_frame_df12_mean.join(cash_type_df_max, "SK_ID_CURR").
//      join(time_frame_df12_min, "SK_ID_CURR").join(time_frame_df12_max, "SK_ID_CURR").
//      join(time_frame_df12_sum, "SK_ID_CURR").show(50)

    val time_frame_df24 = prev.where("DAYS_DECISION >= -720")

    var time_frame_df24_mean = time_frame_df24.groupBy("SK_ID_CURR").mean("AMT_ANNUITY",
      "SIMPLE_INTERESTS", "DAYS_DECISION", "DAYS_LAST_DUE_1ST_VERSION", "APPLICATION_CREDIT_RATIO",
      "NAME_CONTRACT_TYPE_Consumer loans", "NAME_CONTRACT_TYPE_Cash loans", "NAME_CONTRACT_TYPE_Revolving loans")
    Array("AMT_ANNUITY", "SIMPLE_INTERESTS", "DAYS_DECISION", "DAYS_LAST_DUE_1ST_VERSION", "APPLICATION_CREDIT_RATIO",
      "NAME_CONTRACT_TYPE_Consumer loans", "NAME_CONTRACT_TYPE_Cash loans", "NAME_CONTRACT_TYPE_Revolving loans").foreach( x =>
      time_frame_df24_mean = time_frame_df24_mean.withColumnRenamed("avg("+ x +")", "PREV_LAST24M_" + x + "_MEAN")
    )

    var time_frame_df24_min = time_frame_df24.groupBy("SK_ID_CURR").min("DAYS_DECISION", "DAYS_LAST_DUE_1ST_VERSION", "APPLICATION_CREDIT_DIFF", "APPLICATION_CREDIT_RATIO")
    Array("DAYS_DECISION", "DAYS_LAST_DUE_1ST_VERSION", "APPLICATION_CREDIT_DIFF", "APPLICATION_CREDIT_RATIO").foreach( x =>
      time_frame_df24_min = time_frame_df24_min.withColumnRenamed("min("+ x +")", "PREV_LAST24M_" + x + "_MIN")
    )

    var time_frame_df24_max = time_frame_df24.groupBy("SK_ID_CURR").max("AMT_ANNUITY", "SIMPLE_INTERESTS", "DAYS_LAST_DUE_1ST_VERSION", "APPLICATION_CREDIT_RATIO")
    Array("AMT_ANNUITY", "SIMPLE_INTERESTS", "DAYS_LAST_DUE_1ST_VERSION", "APPLICATION_CREDIT_RATIO").foreach( x =>
      time_frame_df24_max = time_frame_df24_max.withColumnRenamed("max("+ x +")", "PREV_LAST24M_" + x + "_MAX")
    )

    var time_frame_df24_sum = time_frame_df24.groupBy("SK_ID_CURR").sum("AMT_CREDIT")
    Array("AMT_CREDIT").foreach( x =>
      time_frame_df24_sum = time_frame_df24_sum.withColumnRenamed("sum("+ x +")", "PREV_LAST24M_" + x + "_SUM")
    )

    agg_prev = agg_prev.join(time_frame_df24_mean.
      join(time_frame_df24_min, "SK_ID_CURR").join(time_frame_df24_max, "SK_ID_CURR").
      join(time_frame_df24_sum, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")

    agg_prev.coalesce(1).repartition(1).write.format("csv").option("header", "false").save("/user/spark/HC1006HC/"+ args(0))
  }

}
