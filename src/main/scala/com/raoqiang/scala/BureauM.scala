package com.raoqiang.scala

import com.raoqiang.scala.ObjectSet.{MyVar, Nunique}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object BureauM {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Bureau M").getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    // 读取 application_train文件，需要去掉TARGET列
    val bureau = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("/user/spark/data/HC1001HC"+ args(0) +".csv")


//    bureau.join(bureau.select($"SK_ID_BUREAU", $"DAYS_CREDIT_ENDDATE" - $"DAYS_CREDIT",
//      $"DAYS_CREDIT_ENDDATE" - $"DAYS_ENDDATE_FACT", $"AMT_CREDIT_SUM" / $"AMT_CREDIT_SUM_DEBT",
//      $"AMT_CREDIT_SUM" - $"AMT_CREDIT_SUM_DEBT", $"AMT_CREDIT_SUM" / $"AMT_ANNUITY"), "SK_ID_BUREAU").show(50)

    val CREDIT_ACTIVE_ONE_HOT_INDEX : Array[String]  = Array("CREDIT_ACTIVE_Active", "CREDIT_ACTIVE_Bad debt",
      "CREDIT_ACTIVE_Closed", "CREDIT_ACTIVE_Sold")

    val CREDIT_CURRENCY_ONE_HOT_INDEX : Array[String]  = Array("CREDIT_CURRENCY_currency 1",
      "CREDIT_CURRENCY_currency 2", "CREDIT_CURRENCY_currency 3", "CREDIT_CURRENCY_currency 4")

    val CREDIT_TYPE_ONE_HOT_INDEX : Array[String]  = Array("CREDIT_TYPE_Another type of loan",
      "CREDIT_TYPE_Car loan", "CREDIT_TYPE_Cash loan (non-earmarked)", "CREDIT_TYPE_Consumer credit",
      "CREDIT_TYPE_Credit card", "CREDIT_TYPE_Interbank credit", "CREDIT_TYPE_Loan for business development",
      "CREDIT_TYPE_Loan for purchase of shares (margin lending)", "CREDIT_TYPE_Loan for the purchase of equipment",
      "CREDIT_TYPE_Loan for working capital replenishment", "CREDIT_TYPE_Microloan",
      "CREDIT_TYPE_Mobile operator loan", "CREDIT_TYPE_Mortgage",
      "CREDIT_TYPE_Real estate loan", "CREDIT_TYPE_Unknown type of loan")

//    bureau.select($"CREDIT_CURRENCY", $"SK_ID_BUREAU").rdd.take(5).foreach(println)

    val caohc = bureau.select($"CREDIT_ACTIVE", $"SK_ID_BUREAU").rdd.map(x =>
      List(x(1)).++(CREDIT_ACTIVE_ONE_HOT_INDEX.map(s => s.equals("CREDIT_ACTIVE_" + x(0)).compareTo(true)+1).toList)
    ).map(row=>Row(row.head, row(1), row(2), row(3), row(4)))

    val CREDIT_CURRENCY_ONE_HOT_INDEX_A = Array("SK_ID_BUREAU", "CREDIT_ACTIVE_Active", "CREDIT_ACTIVE_Bad debt",
      "CREDIT_ACTIVE_Closed", "CREDIT_ACTIVE_Sold").map(fieldName => StructField(fieldName, IntegerType, nullable = true))

    val schema_A = StructType(CREDIT_CURRENCY_ONE_HOT_INDEX_A)

    // rdd 转换成 DataFrame
    val caohcDF = spark.createDataFrame(caohc, schema_A)

    val ccohc = bureau.select($"CREDIT_CURRENCY", $"SK_ID_BUREAU").rdd.map(x =>
      List(x(1)).++(CREDIT_CURRENCY_ONE_HOT_INDEX.map(s => s.equals("CREDIT_CURRENCY_" + x(0)).compareTo(true)+1).toList)
    ).map(row=>Row(row.head, row(1), row(2), row(3), row(4)))

    val CREDIT_CURRENCY_ONE_HOT_INDEX_S = Array("SK_ID_BUREAU", "CREDIT_CURRENCY_currency 1",
      "CREDIT_CURRENCY_currency 2", "CREDIT_CURRENCY_currency 3", "CREDIT_CURRENCY_currency 4").
      map(fieldName => StructField(fieldName, IntegerType, nullable = true))

    val schema_S = StructType(CREDIT_CURRENCY_ONE_HOT_INDEX_S)

    // rdd 转换成 DataFrame
    val ccohcDF = spark.createDataFrame(ccohc, schema_S)

//    ccohcDF.show(5)

    val ctohc = bureau.select($"CREDIT_TYPE", $"SK_ID_BUREAU").rdd.map(x =>
      List(x(1)).++(CREDIT_TYPE_ONE_HOT_INDEX.map(s => s.equals("CREDIT_TYPE_" + x(0)).compareTo(true)+1).toList)).
      map(row=>Row(row.head, row(1), row(2), row(3), row(4), row(5), row(6), row(7),
        row(8), row(9), row(10), row(11), row(12), row(13), row(14), row(15)))


    val CREDIT_CURRENCY_ONE_HOT_INDEX_C = Array("SK_ID_BUREAU", "CREDIT_TYPE_Another type of loan",
      "CREDIT_TYPE_Car loan", "CREDIT_TYPE_Cash loan (non-earmarked)", "CREDIT_TYPE_Consumer credit",
      "CREDIT_TYPE_Credit card", "CREDIT_TYPE_Interbank credit", "CREDIT_TYPE_Loan for business development",
      "CREDIT_TYPE_Loan for purchase of shares (margin lending)", "CREDIT_TYPE_Loan for the purchase of equipment",
      "CREDIT_TYPE_Loan for working capital replenishment", "CREDIT_TYPE_Microloan",
      "CREDIT_TYPE_Mobile operator loan", "CREDIT_TYPE_Mortgage",
      "CREDIT_TYPE_Real estate loan", "CREDIT_TYPE_Unknown type of loan").map(fieldName => StructField(fieldName, IntegerType, nullable = true))

    val schema_C = StructType(CREDIT_CURRENCY_ONE_HOT_INDEX_C)

    // rdd 转换成 DataFrame
    val ctohcDF = spark.createDataFrame(ctohc, schema_C)

//    ctohcDF.show(5)

    val bureauProcess = bureau.join(bureau.select($"SK_ID_BUREAU", $"DAYS_CREDIT_ENDDATE" - $"DAYS_CREDIT",
      $"DAYS_CREDIT_ENDDATE" - $"DAYS_ENDDATE_FACT", $"AMT_CREDIT_SUM" / $"AMT_CREDIT_SUM_DEBT",
      $"AMT_CREDIT_SUM" - $"AMT_CREDIT_SUM_DEBT", $"AMT_CREDIT_SUM" / $"AMT_ANNUITY"), "SK_ID_BUREAU")
      .join(caohcDF, "SK_ID_BUREAU")
      .join(ccohcDF, "SK_ID_BUREAU")
      .join(ctohcDF, "SK_ID_BUREAU")
      .withColumnRenamed("(DAYS_CREDIT_ENDDATE - DAYS_CREDIT)","CREDIT_DURATION")
      .withColumnRenamed("(DAYS_CREDIT_ENDDATE - DAYS_ENDDATE_FACT)","ENDDATE_DIF")
      .withColumnRenamed("(AMT_CREDIT_SUM / AMT_CREDIT_SUM_DEBT)","DEBT_PERCENTAGE")
      .withColumnRenamed("(AMT_CREDIT_SUM - AMT_CREDIT_SUM_DEBT)","DEBT_CREDIT_DIFF")
      .withColumnRenamed("(AMT_CREDIT_SUM / AMT_ANNUITY)","CREDIT_TO_ANNUITY_RATIO")
      .drop("CREDIT_ACTIVE", "CREDIT_CURRENCY", "CREDIT_TYPE")
//      .show(50)



    val bureauBalance = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("/user/spark/data/HC1002HC"+ args(0) +".csv")


    val STATUS_ONE_HOT_INDEX = Array("STATUS_0", "STATUS_1", "STATUS_2", "STATUS_3", "STATUS_4", "STATUS_5", "STATUS_C", "STATUS_X")

    val bbrdd = bureauBalance.rdd.map(row => Row(row(0)+"", row(1)+"", row(2)+"", ("0"==row(2)).compareTo(true)+1+"",
      ("1"==row(2)).compareTo(true)+1+"", ("2"==row(2)).compareTo(true)+1+"",
      ("3"==row(2)).compareTo(true)+1+"", ("4"==row(2)).compareTo(true)+1+"",
      ("5"==row(2)).compareTo(true)+1+"", ("C"==row(2)).compareTo(true)+1+"",
      ("X"==row(2)).compareTo(true)+1+""))

    // 字段类型，字段名称判断是不是为空
    val fields = Array("SK_ID_BUREAU", "MONTHS_BALANCE", "STATUS", "STATUS_0", "STATUS_1", "STATUS_2", "STATUS_3",
      "STATUS_4", "STATUS_5", "STATUS_C", "STATUS_X").map(fieldName => StructField(fieldName, StringType, nullable = true))


    val schema = StructType(fields)

    val bbDFGB = spark.createDataFrame(bbrdd, schema).select($"SK_ID_BUREAU", $"MONTHS_BALANCE".cast(IntegerType), $"STATUS",
      $"STATUS_0".cast(IntegerType), $"STATUS_1".cast(IntegerType), $"STATUS_2".cast(IntegerType),
      $"STATUS_3".cast(IntegerType), $"STATUS_4".cast(IntegerType), $"STATUS_5".cast(IntegerType),
      $"STATUS_C".cast(IntegerType), $"STATUS_X".cast(IntegerType))

//      .mean("STATUS_0", "STATUS_1", "STATUS_2", "STATUS_3", "STATUS_4", "STATUS_5", "STATUS_C", "STATUS_X")
    val meanDF = bbDFGB.groupBy("SK_ID_BUREAU").mean("STATUS_0", "STATUS_1", "STATUS_2", "STATUS_3",
      "STATUS_4", "STATUS_5", "STATUS_C", "STATUS_X")
        .withColumnRenamed("avg(STATUS_0)", "STATUS_0")
        .withColumnRenamed("avg(STATUS_1)", "STATUS_1")
        .withColumnRenamed("avg(STATUS_2)", "STATUS_2")
        .withColumnRenamed("avg(STATUS_3)", "STATUS_3")
        .withColumnRenamed("avg(STATUS_4)", "STATUS_4")
        .withColumnRenamed("avg(STATUS_5)", "STATUS_5")
        .withColumnRenamed("avg(STATUS_C)", "STATUS_C")
        .withColumnRenamed("avg(STATUS_X)", "STATUS_X")

//    meanDF.show(5)

    val mbMinDF = bbDFGB.groupBy("SK_ID_BUREAU").min("MONTHS_BALANCE")
      .withColumnRenamed("min(MONTHS_BALANCE)", "MONTHS_BALANCE_MIN")

    val mbMaxDF = bbDFGB.groupBy("SK_ID_BUREAU").max("MONTHS_BALANCE")
      .withColumnRenamed("max(MONTHS_BALANCE)", "MONTHS_BALANCE_MAX")

    val mbMeanDF = bbDFGB.groupBy("SK_ID_BUREAU").mean("MONTHS_BALANCE")
      .withColumnRenamed("avg(MONTHS_BALANCE)", "MONTHS_BALANCE_MEAN")

    val mbSizeDF = bbDFGB.groupBy("SK_ID_BUREAU").agg(Map("MONTHS_BALANCE" -> "count"))
      .withColumnRenamed("count(MONTHS_BALANCE)", "MONTHS_BALANCE_SIZE")

//    val mbMMMS = bbDFGB.s

//    mbMinDF.show(5)
//    mbMaxDF.show(5)
//    mbMeanDF.show(5)
//    mbSizeDF.show(5)

    val bbProcess = meanDF.join(mbMinDF, "SK_ID_BUREAU")
      .join(mbMaxDF, "SK_ID_BUREAU")
      .join(mbMeanDF, "SK_ID_BUREAU")
      .join(mbSizeDF, "SK_ID_BUREAU").orderBy("SK_ID_BUREAU")

    val bMbb_ = bureauProcess.join(bbProcess, Array("SK_ID_BUREAU"), "left")

    val bMbb = bMbb_.join(bMbb_.select($"SK_ID_BUREAU", $"STATUS_1" + $"STATUS_2" +
      $"STATUS_3" + $"STATUS_4" + $"STATUS_5"), "SK_ID_BUREAU").withColumnRenamed("((((STATUS_1 + STATUS_2) + STATUS_3) + STATUS_4) + STATUS_5)", "STATUS_12345")

    val feature_mean = Array("AMT_CREDIT_MAX_OVERDUE", "AMT_CREDIT_SUM_OVERDUE", "AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT", "DEBT_PERCENTAGE", "DEBT_CREDIT_DIFF", "STATUS_0", "STATUS_12345")

    val tmp = bMbb.join(bMbb.groupBy("MONTHS_BALANCE_SIZE").mean("AMT_CREDIT_MAX_OVERDUE", "AMT_CREDIT_SUM_OVERDUE", "AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT", "DEBT_PERCENTAGE", "DEBT_CREDIT_DIFF", "STATUS_0", "STATUS_12345")
      .withColumnRenamed("avg(AMT_CREDIT_MAX_OVERDUE)", "LL_AMT_CREDIT_MAX_OVERDUE")
      .withColumnRenamed("avg(AMT_CREDIT_SUM_OVERDUE)", "LL_AMT_CREDIT_SUM_OVERDUE")
      .withColumnRenamed("avg(AMT_CREDIT_SUM)", "LL_AMT_CREDIT_SUM")
      .withColumnRenamed("avg(AMT_CREDIT_SUM_DEBT)", "LL_AMT_CREDIT_SUM_DEBT")
      .withColumnRenamed("avg(DEBT_PERCENTAGE)", "LL_DEBT_PERCENTAGE")
      .withColumnRenamed("avg(DEBT_CREDIT_DIFF)", "LL_DEBT_CREDIT_DIFF")
      .withColumnRenamed("avg(STATUS_0)", "LL_STATUS_0")
      .withColumnRenamed("avg(STATUS_12345)", "LL_STATUS_12345"), Array("MONTHS_BALANCE_SIZE"), "left")

    val tmp_mean = tmp.orderBy("SK_ID_CURR").groupBy("SK_ID_CURR").mean("DAYS_CREDIT", "AMT_CREDIT_MAX_OVERDUE",
      "AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT", "AMT_CREDIT_SUM_OVERDUE", "AMT_ANNUITY", "DEBT_CREDIT_DIFF",
      "MONTHS_BALANCE_MEAN", "MONTHS_BALANCE_SIZE", "STATUS_0", "STATUS_1", "STATUS_12345", "STATUS_C", "STATUS_X",
      "CREDIT_ACTIVE_Active", "CREDIT_ACTIVE_Closed", "CREDIT_ACTIVE_Sold", "CREDIT_TYPE_Consumer credit", "CREDIT_TYPE_Credit card",
      "CREDIT_TYPE_Car loan", "CREDIT_TYPE_Mortgage", "CREDIT_TYPE_Microloan", "LL_AMT_CREDIT_SUM_OVERDUE", "LL_DEBT_CREDIT_DIFF", "LL_STATUS_12345")
      .withColumnRenamed("avg(DAYS_CREDIT)", "BUREAU_"+"DAYS_CREDIT"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_MAX_OVERDUE)", "BUREAU_"+"AMT_CREDIT_MAX_OVERDUE"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM)", "BUREAU_"+"AMT_CREDIT_SUM"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM_DEBT)", "BUREAU_"+"AMT_CREDIT_SUM_DEBT"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM_OVERDUE)", "BUREAU_"+"AMT_CREDIT_SUM_OVERDUE"+"_MEAN")
      .withColumnRenamed("avg(AMT_ANNUITY)", "BUREAU_"+"AMT_ANNUITY"+"_MEAN")
      .withColumnRenamed("avg(DEBT_CREDIT_DIFF)", "BUREAU_"+"DEBT_CREDIT_DIFF"+"_MEAN")
      .withColumnRenamed("avg(MONTHS_BALANCE_MEAN)", "BUREAU_"+"MONTHS_BALANCE_MEAN"+"_MEAN")
      .withColumnRenamed("avg(MONTHS_BALANCE_SIZE)", "BUREAU_"+"MONTHS_BALANCE_SIZE"+"_MEAN")
      .withColumnRenamed("avg(STATUS_0)", "BUREAU_"+"STATUS_0"+"_MEAN")
      .withColumnRenamed("avg(STATUS_1)", "BUREAU_"+"STATUS_1"+"_MEAN")
      .withColumnRenamed("avg(STATUS_12345)", "BUREAU_"+"STATUS_12345"+"_MEAN")
      .withColumnRenamed("avg(STATUS_C)", "BUREAU_"+"STATUS_C"+"_MEAN")
      .withColumnRenamed("avg(STATUS_X)", "BUREAU_"+"STATUS_X"+"_MEAN")
      .withColumnRenamed("avg(CREDIT_ACTIVE_Active)", "BUREAU_"+"CREDIT_ACTIVE_Active"+"_MEAN")
      .withColumnRenamed("avg(CREDIT_ACTIVE_Closed)", "BUREAU_"+"CREDIT_ACTIVE_Closed"+"_MEAN")
      .withColumnRenamed("avg(CREDIT_ACTIVE_Sold)", "BUREAU_"+"CREDIT_ACTIVE_Sold"+"_MEAN")
      .withColumnRenamed("avg(CREDIT_TYPE_Consumer credit)", "BUREAU_"+"CREDIT_TYPE_Consumer credit"+"_MEAN")
      .withColumnRenamed("avg(CREDIT_TYPE_Credit card)", "BUREAU_"+"CREDIT_TYPE_Credit card"+"_MEAN")
      .withColumnRenamed("avg(CREDIT_TYPE_Car loan)", "BUREAU_"+"CREDIT_TYPE_Car loan"+"_MEAN")
      .withColumnRenamed("avg(CREDIT_TYPE_Mortgage)", "BUREAU_"+"CREDIT_TYPE_Mortgage"+"_MEAN")
      .withColumnRenamed("avg(CREDIT_TYPE_Microloan)", "BUREAU_"+"CREDIT_TYPE_Microloan"+"_MEAN")
      .withColumnRenamed("avg(LL_AMT_CREDIT_SUM_OVERDUE)", "BUREAU_"+"LL_AMT_CREDIT_SUM_OVERDUE"+"_MEAN")
      .withColumnRenamed("avg(LL_DEBT_CREDIT_DIFF)", "BUREAU_"+"LL_DEBT_CREDIT_DIFF"+"_MEAN")
      .withColumnRenamed("avg(LL_STATUS_12345)", "BUREAU_"+"LL_STATUS_12345"+"_MEAN")

    val tmp_max = tmp.orderBy("SK_ID_CURR").groupBy("SK_ID_CURR").max("DAYS_CREDIT",
      "DAYS_CREDIT_ENDDATE", "AMT_CREDIT_MAX_OVERDUE", "AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT", "AMT_CREDIT_SUM_OVERDUE")
      .withColumnRenamed("max(DAYS_CREDIT)", "BUREAU_"+"DAYS_CREDIT"+"_MAX")
      .withColumnRenamed("max(DAYS_CREDIT_ENDDATE)", "BUREAU_"+"DAYS_CREDIT_ENDDATE"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_MAX_OVERDUE)", "BUREAU_"+"AMT_CREDIT_MAX_OVERDUE"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM)", "BUREAU_"+"AMT_CREDIT_SUM"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM_DEBT)", "BUREAU_"+"AMT_CREDIT_SUM_DEBT"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM_OVERDUE)", "BUREAU_"+"AMT_CREDIT_SUM_OVERDUE"+"_MAX")


    val tmp_min = tmp.orderBy("SK_ID_CURR").groupBy("SK_ID_CURR").min("DAYS_CREDIT", "DAYS_CREDIT_ENDDATE")
      .withColumnRenamed("min(DAYS_CREDIT)", "BUREAU_"+"DAYS_CREDIT"+"_MIN")
      .withColumnRenamed("min(DAYS_CREDIT_ENDDATE)", "BUREAU_"+"DAYS_CREDIT_ENDDATE"+"_MIN")


    val tmp_sum = tmp.orderBy("SK_ID_CURR").groupBy("SK_ID_CURR").sum("AMT_CREDIT_SUM",
      "AMT_CREDIT_SUM_DEBT", "AMT_CREDIT_SUM_OVERDUE", "DEBT_CREDIT_DIFF", "MONTHS_BALANCE_SIZE")
      .withColumnRenamed("sum(AMT_CREDIT_SUM)", "BUREAU_"+"AMT_CREDIT_SUM"+"_SUM")
      .withColumnRenamed("sum(AMT_CREDIT_SUM_DEBT)", "BUREAU_"+"AMT_CREDIT_SUM_DEBT"+"_SUM")
      .withColumnRenamed("sum(AMT_CREDIT_SUM_OVERDUE)", "BUREAU_"+"AMT_CREDIT_SUM_OVERDUE"+"_SUM")
      .withColumnRenamed("sum(DEBT_CREDIT_DIFF)", "BUREAU_"+"DEBT_CREDIT_DIFF"+"_SUM")
      .withColumnRenamed("sum(MONTHS_BALANCE_SIZE)", "BUREAU_"+"MONTHS_BALANCE_SIZE"+"_SUM")

    spark.udf.register("myVar", MyVar)
    spark.udf.register("nunique", Nunique)

    val tmp_var = tmp.orderBy("SK_ID_CURR").groupBy("SK_ID_CURR").agg(Map("MONTHS_BALANCE_MEAN" -> "MyVar"))
      .withColumnRenamed("myvar(MONTHS_BALANCE_MEAN)", "BUREAU_"+"MONTHS_BALANCE_MEAN"+"_VAR")

    val tmp_nunique = tmp.orderBy("SK_ID_CURR").groupBy("SK_ID_CURR").agg(Map("SK_ID_BUREAU" -> "nunique"))
      .withColumnRenamed("nunique(SK_ID_BUREAU)", "BUREAU_"+"SK_ID_BUREAU"+"_NUNIQUE")


    var agg_bureau = tmp_mean.join(tmp_sum, "SK_ID_CURR").join(tmp_nunique, "SK_ID_CURR").
      join(tmp_max, "SK_ID_CURR").join(tmp_min, "SK_ID_CURR").join(tmp_var, "SK_ID_CURR")

    val active_bureau = tmp.where("CREDIT_ACTIVE_Active=1")


    val active_mean = active_bureau.groupBy("SK_ID_CURR").mean("DAYS_CREDIT", "AMT_CREDIT_MAX_OVERDUE",
      "AMT_CREDIT_SUM_DEBT", "AMT_CREDIT_SUM_OVERDUE", "DAYS_CREDIT_UPDATE", "DEBT_PERCENTAGE", "DEBT_CREDIT_DIFF",
      "CREDIT_TO_ANNUITY_RATIO", "MONTHS_BALANCE_MEAN", "MONTHS_BALANCE_SIZE")
      .withColumnRenamed("avg(DAYS_CREDIT)","BUREAU_ACTIVE_"+"DAYS_CREDIT"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_MAX_OVERDUE)","BUREAU_ACTIVE_"+"AMT_CREDIT_MAX_OVERDUE"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM_DEBT)","BUREAU_ACTIVE_"+"AMT_CREDIT_SUM_DEBT"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM_OVERDUE)","BUREAU_ACTIVE_"+"AMT_CREDIT_SUM_OVERDUE"+"_MEAN")
      .withColumnRenamed("avg(DAYS_CREDIT_UPDATE)","BUREAU_ACTIVE_"+"DAYS_CREDIT_UPDATE"+"_MEAN")
      .withColumnRenamed("avg(DEBT_PERCENTAGE)","BUREAU_ACTIVE_"+"DEBT_PERCENTAGE"+"_MEAN")
      .withColumnRenamed("avg(DEBT_CREDIT_DIFF)","BUREAU_ACTIVE_"+"DEBT_CREDIT_DIFF"+"_MEAN")
      .withColumnRenamed("avg(CREDIT_TO_ANNUITY_RATIO)","BUREAU_ACTIVE_"+"CREDIT_TO_ANNUITY_RATIO"+"_MEAN")
      .withColumnRenamed("avg(MONTHS_BALANCE_MEAN)","BUREAU_ACTIVE_"+"MONTHS_BALANCE_MEAN"+"_MEAN")
      .withColumnRenamed("avg(MONTHS_BALANCE_SIZE)","BUREAU_ACTIVE_"+"MONTHS_BALANCE_SIZE"+"_MEAN")


    val active_max = active_bureau.groupBy("SK_ID_CURR").max("DAYS_CREDIT", "DAYS_CREDIT_ENDDATE",
      "AMT_CREDIT_MAX_OVERDUE", "AMT_CREDIT_SUM", "AMT_CREDIT_SUM_OVERDUE")
      .withColumnRenamed("max(DAYS_CREDIT)","BUREAU_ACTIVE_"+"DAYS_CREDIT"+"_MAX")
      .withColumnRenamed("max(DAYS_CREDIT_ENDDATE)","BUREAU_ACTIVE_"+"DAYS_CREDIT_ENDDATE"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_MAX_OVERDUE)","BUREAU_ACTIVE_"+"AMT_CREDIT_MAX_OVERDUE"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM)","BUREAU_ACTIVE_"+"AMT_CREDIT_SUM"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM_OVERDUE)","BUREAU_ACTIVE_"+"AMT_CREDIT_SUM_OVERDUE"+"_MAX")


    val active_min = active_bureau.groupBy("SK_ID_CURR").min("DAYS_CREDIT_ENDDATE", "DAYS_CREDIT_UPDATE")
      .withColumnRenamed("min(DAYS_CREDIT_ENDDATE)","BUREAU_ACTIVE_"+"DAYS_CREDIT_ENDDATE"+"_MIN")
      .withColumnRenamed("min(DAYS_CREDIT_UPDATE)","BUREAU_ACTIVE_"+"DAYS_CREDIT_UPDATE"+"_MIN")


    val active_sum = active_bureau.groupBy("SK_ID_CURR").sum("AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT", "MONTHS_BALANCE_SIZE")
      .withColumnRenamed("sum(AMT_CREDIT_SUM)","BUREAU_ACTIVE_"+"AMT_CREDIT_SUM"+"_SUM")
      .withColumnRenamed("sum(AMT_CREDIT_SUM_DEBT)","BUREAU_ACTIVE_"+"AMT_CREDIT_SUM_DEBT"+"_SUM")
      .withColumnRenamed("sum(MONTHS_BALANCE_SIZE)","BUREAU_ACTIVE_"+"MONTHS_BALANCE_SIZE"+"_SUM")

    val active_var = active_bureau.groupBy("SK_ID_CURR").agg(Map("MONTHS_BALANCE_MEAN" -> "myVar"))
      .withColumnRenamed("myVar(MONTHS_BALANCE_MEAN)","BUREAU_ACTIVE_"+"MONTHS_BALANCE_MEAN"+"_VAR")


    agg_bureau = agg_bureau.join(active_mean.join(active_max, "SK_ID_CURR").join(active_min, "SK_ID_CURR").join(active_sum, "SK_ID_CURR").join(active_var, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")


    val closed_bureau = tmp.where("CREDIT_ACTIVE_Closed=1")

    val closed_mean = closed_bureau.groupBy("SK_ID_CURR").mean("AMT_CREDIT_MAX_OVERDUE", "AMT_CREDIT_SUM_OVERDUE",
      "AMT_CREDIT_SUM", "ENDDATE_DIF", "STATUS_12345")
      .withColumnRenamed("avg(AMT_CREDIT_MAX_OVERDUE)","BUREAU_CLOSED_"+"AMT_CREDIT_MAX_OVERDUE"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM_OVERDUE)","BUREAU_CLOSED_"+"AMT_CREDIT_SUM_OVERDUE"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM)","BUREAU_CLOSED_"+"AMT_CREDIT_SUM"+"_MEAN")
      .withColumnRenamed("avg(ENDDATE_DIF)","BUREAU_CLOSED_"+"ENDDATE_DIF"+"_MEAN")
      .withColumnRenamed("avg(STATUS_12345)","BUREAU_CLOSED_"+"STATUS_12345"+"_MEAN")


    val closed_max = closed_bureau.groupBy("SK_ID_CURR").max("DAYS_CREDIT", "DAYS_CREDIT_ENDDATE",
      "AMT_CREDIT_MAX_OVERDUE", "AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT", "DAYS_CREDIT_UPDATE")
      .withColumnRenamed("max(DAYS_CREDIT)","BUREAU_CLOSED_"+"DAYS_CREDIT"+"_MAX")
      .withColumnRenamed("max(DAYS_CREDIT_ENDDATE)","BUREAU_CLOSED_"+"DAYS_CREDIT_ENDDATE"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_MAX_OVERDUE)","BUREAU_CLOSED_"+"AMT_CREDIT_MAX_OVERDUE"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM)","BUREAU_CLOSED_"+"AMT_CREDIT_SUM"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM_DEBT)","BUREAU_CLOSED_"+"AMT_CREDIT_SUM_DEBT"+"_MAX")
      .withColumnRenamed("max(DAYS_CREDIT_UPDATE)","BUREAU_CLOSED_"+"DAYS_CREDIT_UPDATE"+"_MAX")


//    val closed_min = closed_bureau.groupBy("SK_ID_CURR").min("DAYS_CREDIT_ENDDATE", "DAYS_CREDIT_UPDATE")
//      .withColumnRenamed("min(DAYS_CREDIT_ENDDATE)","BUREAU_CLOSED_"+"DAYS_CREDIT_ENDDATE"+"_MIN")
//      .withColumnRenamed("min(DAYS_CREDIT_UPDATE)","BUREAU_CLOSED_"+"DAYS_CREDIT_UPDATE"+"_MIN")

    val closed_sum = closed_bureau.groupBy("SK_ID_CURR").sum("AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT")
      .withColumnRenamed("sum(AMT_CREDIT_SUM)","BUREAU_CLOSED_"+"AMT_CREDIT_SUM"+"_SUM")
      .withColumnRenamed("sum(AMT_CREDIT_SUM_DEBT)","BUREAU_CLOSED_"+"AMT_CREDIT_SUM_DEBT"+"_SUM")

    val closed_var = closed_bureau.groupBy("SK_ID_CURR").agg(Map("DAYS_CREDIT" -> "myVar"))
      .withColumnRenamed("myVar(DAYS_CREDIT)","BUREAU_CLOSED_"+"DAYS_CREDIT"+"_VAR")


    agg_bureau = agg_bureau.join(closed_mean.join(closed_max, "SK_ID_CURR").join(closed_sum, "SK_ID_CURR").join(closed_var, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")

//    agg_bureau.orderBy("SK_ID_CURR").show(50)



    val Consumer_bureau = tmp.where($"CREDIT_TYPE_Consumer credit"===1)

    val Consumer_mean = Consumer_bureau.groupBy("SK_ID_CURR").mean("DAYS_CREDIT", "AMT_CREDIT_MAX_OVERDUE",
    "AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT", "DEBT_PERCENTAGE", "DEBT_CREDIT_DIFF")
      .withColumnRenamed("avg(DAYS_CREDIT)","BUREAU_CONSUMER_"+"DAYS_CREDIT"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_MAX_OVERDUE)","BUREAU_CONSUMER_"+"AMT_CREDIT_MAX_OVERDUE"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM)","BUREAU_CONSUMER_"+"AMT_CREDIT_SUM"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM_DEBT)","BUREAU_CONSUMER_"+"AMT_CREDIT_SUM_DEBT"+"_MEAN")
      .withColumnRenamed("avg(DEBT_PERCENTAGE)","BUREAU_CONSUMER_"+"DEBT_PERCENTAGE"+"_MEAN")
      .withColumnRenamed("avg(DEBT_CREDIT_DIFF)","BUREAU_CONSUMER_"+"DEBT_CREDIT_DIFF"+"_MEAN")


    val Consumer_max = Consumer_bureau.groupBy("SK_ID_CURR").max("DAYS_CREDIT", "AMT_CREDIT_MAX_OVERDUE",
    "AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT", "DAYS_CREDIT_ENDDATE")
      .withColumnRenamed("max(DAYS_CREDIT)", "BUREAU_CONSUMER_"+"DAYS_CREDIT"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_MAX_OVERDUE)", "BUREAU_CONSUMER_"+"AMT_CREDIT_MAX_OVERDUE"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM)", "BUREAU_CONSUMER_"+"AMT_CREDIT_SUM"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM_DEBT)", "BUREAU_CONSUMER_"+"AMT_CREDIT_SUM_DEBT"+"_MAX")
      .withColumnRenamed("max(DAYS_CREDIT_ENDDATE)", "BUREAU_CONSUMER_"+"DAYS_CREDIT_ENDDATE"+"_MAX")


    agg_bureau = agg_bureau.join(Consumer_mean.join(Consumer_max, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")


    val Credit_bureau = tmp.where($"CREDIT_TYPE_Credit card"===1)


    val Credit_mean = Credit_bureau.groupBy("SK_ID_CURR").mean("DAYS_CREDIT", "AMT_CREDIT_MAX_OVERDUE",
      "AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT", "DEBT_PERCENTAGE", "DEBT_CREDIT_DIFF")
      .withColumnRenamed("avg(DAYS_CREDIT)","BUREAU_CREDIT_"+"DAYS_CREDIT"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_MAX_OVERDUE)","BUREAU_CREDIT_"+"AMT_CREDIT_MAX_OVERDUE"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM)","BUREAU_CREDIT_"+"AMT_CREDIT_SUM"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM_DEBT)","BUREAU_CREDIT_"+"AMT_CREDIT_SUM_DEBT"+"_MEAN")
      .withColumnRenamed("avg(DEBT_PERCENTAGE)","BUREAU_CREDIT_"+"DEBT_PERCENTAGE"+"_MEAN")
      .withColumnRenamed("avg(DEBT_CREDIT_DIFF)","BUREAU_CREDIT_"+"DEBT_CREDIT_DIFF"+"_MEAN")


    val Credit_max = Credit_bureau.groupBy("SK_ID_CURR").max("DAYS_CREDIT", "AMT_CREDIT_MAX_OVERDUE",
      "AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT", "DAYS_CREDIT_ENDDATE")
      .withColumnRenamed("max(DAYS_CREDIT)", "BUREAU_CREDIT_"+"DAYS_CREDIT"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_MAX_OVERDUE)", "BUREAU_CREDIT_"+"AMT_CREDIT_MAX_OVERDUE"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM)", "BUREAU_CREDIT_"+"AMT_CREDIT_SUM"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM_DEBT)", "BUREAU_CREDIT_"+"AMT_CREDIT_SUM_DEBT"+"_MAX")
      .withColumnRenamed("max(DAYS_CREDIT_ENDDATE)", "BUREAU_CREDIT_"+"DAYS_CREDIT_ENDDATE"+"_MAX")


    agg_bureau = agg_bureau.join(Credit_mean.join(Credit_max, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")


    val Mortgage_bureau = tmp.where("CREDIT_TYPE_Mortgage=1")


    val Mortgage_mean = Mortgage_bureau.groupBy("SK_ID_CURR").mean("DAYS_CREDIT", "AMT_CREDIT_MAX_OVERDUE",
      "AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT", "DEBT_PERCENTAGE", "DEBT_CREDIT_DIFF")
      .withColumnRenamed("avg(DAYS_CREDIT)","BUREAU_MORTGAGE_"+"DAYS_CREDIT"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_MAX_OVERDUE)","BUREAU_MORTGAGE_"+"AMT_CREDIT_MAX_OVERDUE"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM)","BUREAU_MORTGAGE_"+"AMT_CREDIT_SUM"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM_DEBT)","BUREAU_MORTGAGE_"+"AMT_CREDIT_SUM_DEBT"+"_MEAN")
      .withColumnRenamed("avg(DEBT_PERCENTAGE)","BUREAU_MORTGAGE_"+"DEBT_PERCENTAGE"+"_MEAN")
      .withColumnRenamed("avg(DEBT_CREDIT_DIFF)","BUREAU_MORTGAGE_"+"DEBT_CREDIT_DIFF"+"_MEAN")


    val Mortgage_max = Mortgage_bureau.groupBy("SK_ID_CURR").max("DAYS_CREDIT", "AMT_CREDIT_MAX_OVERDUE",
      "AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT", "DAYS_CREDIT_ENDDATE")
      .withColumnRenamed("max(DAYS_CREDIT)", "BUREAU_MORTGAGE_"+"DAYS_CREDIT"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_MAX_OVERDUE)", "BUREAU_MORTGAGE_"+"AMT_CREDIT_MAX_OVERDUE"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM)", "BUREAU_MORTGAGE_"+"AMT_CREDIT_SUM"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM_DEBT)", "BUREAU_MORTGAGE_"+"AMT_CREDIT_SUM_DEBT"+"_MAX")
      .withColumnRenamed("max(DAYS_CREDIT_ENDDATE)", "BUREAU_MORTGAGE_"+"DAYS_CREDIT_ENDDATE"+"_MAX")


    agg_bureau = agg_bureau.join(Mortgage_mean.join(Mortgage_max, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")

    val Car_bureau = tmp.where($"CREDIT_TYPE_Car loan"===1)


    val Car_mean = Car_bureau.groupBy("SK_ID_CURR").mean("DAYS_CREDIT", "AMT_CREDIT_MAX_OVERDUE",
      "AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT", "DEBT_PERCENTAGE", "DEBT_CREDIT_DIFF")
      .withColumnRenamed("avg(DAYS_CREDIT)","BUREAU_CAR_"+"DAYS_CREDIT"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_MAX_OVERDUE)","BUREAU_CAR_"+"AMT_CREDIT_MAX_OVERDUE"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM)","BUREAU_CAR_"+"AMT_CREDIT_SUM"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM_DEBT)","BUREAU_CAR_"+"AMT_CREDIT_SUM_DEBT"+"_MEAN")
      .withColumnRenamed("avg(DEBT_PERCENTAGE)","BUREAU_CAR_"+"DEBT_PERCENTAGE"+"_MEAN")
      .withColumnRenamed("avg(DEBT_CREDIT_DIFF)","BUREAU_CAR_"+"DEBT_CREDIT_DIFF"+"_MEAN")


    val Car_max = Car_bureau.groupBy("SK_ID_CURR").max("DAYS_CREDIT", "AMT_CREDIT_MAX_OVERDUE",
      "AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT", "DAYS_CREDIT_ENDDATE")
      .withColumnRenamed("max(DAYS_CREDIT)", "BUREAU_CAR_"+"DAYS_CREDIT"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_MAX_OVERDUE)", "BUREAU_CAR_"+"AMT_CREDIT_MAX_OVERDUE"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM)", "BUREAU_CAR_"+"AMT_CREDIT_SUM"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM_DEBT)", "BUREAU_CAR_"+"AMT_CREDIT_SUM_DEBT"+"_MAX")
      .withColumnRenamed("max(DAYS_CREDIT_ENDDATE)", "BUREAU_CAR_"+"DAYS_CREDIT_ENDDATE"+"_MAX")

    agg_bureau = agg_bureau.join(Car_mean.join(Car_max, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")


    val Microloan_bureau = tmp.where("CREDIT_TYPE_Microloan=1")


    val Microloan_mean = Microloan_bureau.groupBy("SK_ID_CURR").mean("DAYS_CREDIT", "AMT_CREDIT_MAX_OVERDUE",
      "AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT", "DEBT_PERCENTAGE", "DEBT_CREDIT_DIFF")
      .withColumnRenamed("avg(DAYS_CREDIT)","BUREAU_MICROLOAN_"+"DAYS_CREDIT"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_MAX_OVERDUE)","BUREAU_MICROLOAN_"+"AMT_CREDIT_MAX_OVERDUE"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM)","BUREAU_MICROLOAN_"+"AMT_CREDIT_SUM"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM_DEBT)","BUREAU_MICROLOAN_"+"AMT_CREDIT_SUM_DEBT"+"_MEAN")
      .withColumnRenamed("avg(DEBT_PERCENTAGE)","BUREAU_MICROLOAN_"+"DEBT_PERCENTAGE"+"_MEAN")
      .withColumnRenamed("avg(DEBT_CREDIT_DIFF)","BUREAU_MICROLOAN_"+"DEBT_CREDIT_DIFF"+"_MEAN")


    val Microloan_max = Microloan_bureau.groupBy("SK_ID_CURR").max("DAYS_CREDIT", "AMT_CREDIT_MAX_OVERDUE",
      "AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT", "DAYS_CREDIT_ENDDATE")
      .withColumnRenamed("max(DAYS_CREDIT)", "BUREAU_MICROLOAN_"+"DAYS_CREDIT"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_MAX_OVERDUE)", "BUREAU_MICROLOAN_"+"AMT_CREDIT_MAX_OVERDUE"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM)", "BUREAU_MICROLOAN_"+"AMT_CREDIT_SUM"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM_DEBT)", "BUREAU_MICROLOAN_"+"AMT_CREDIT_SUM_DEBT"+"_MAX")
      .withColumnRenamed("max(DAYS_CREDIT_ENDDATE)", "BUREAU_MICROLOAN_"+"DAYS_CREDIT_ENDDATE"+"_MAX")

    agg_bureau = agg_bureau.join(Microloan_mean.join(Microloan_max, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")

//    agg_bureau.show(1000)

    val DAYS180 = tmp.where("DAYS_CREDIT >= -180")

    val DAYS180_mean = DAYS180.groupBy("SK_ID_CURR").mean("AMT_CREDIT_MAX_OVERDUE", "AMT_CREDIT_SUM_OVERDUE",
    "AMT_CREDIT_SUM_DEBT", "DEBT_PERCENTAGE", "DEBT_CREDIT_DIFF", "STATUS_0", "STATUS_12345")
      .withColumnRenamed("avg(AMT_CREDIT_MAX_OVERDUE)", "BUREAU_LAST6M_"+"AMT_CREDIT_MAX_OVERDUE"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM_OVERDUE)", "BUREAU_LAST6M_"+"AMT_CREDIT_SUM_OVERDUE"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM_DEBT)", "BUREAU_LAST6M_"+"AMT_CREDIT_SUM_DEBT"+"_MEAN")
      .withColumnRenamed("avg(DEBT_PERCENTAGE)", "BUREAU_LAST6M_"+"DEBT_PERCENTAGE"+"_MEAN")
      .withColumnRenamed("avg(DEBT_CREDIT_DIFF)", "BUREAU_LAST6M_"+"DEBT_CREDIT_DIFF"+"_MEAN")
      .withColumnRenamed("avg(STATUS_0)", "BUREAU_LAST6M_"+"STATUS_0"+"_MEAN")
      .withColumnRenamed("avg(STATUS_12345)", "BUREAU_LAST6M_"+"STATUS_12345"+"_MEAN")

    val DAYS180_max = DAYS180.groupBy("SK_ID_CURR").max("AMT_CREDIT_MAX_OVERDUE", "AMT_CREDIT_SUM")
      .withColumnRenamed("max(AMT_CREDIT_MAX_OVERDUE)", "BUREAU_LAST6M_"+"AMT_CREDIT_MAX_OVERDUE"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM)", "BUREAU_LAST6M_"+"AMT_CREDIT_SUM"+"_MAX")

    val DAYS180_sum = DAYS180.groupBy("SK_ID_CURR").sum("AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT")
      .withColumnRenamed("sum(AMT_CREDIT_SUM)", "BUREAU_LAST6M_"+"AMT_CREDIT_SUM"+"_SUM")
      .withColumnRenamed("sum(AMT_CREDIT_SUM_DEBT)", "BUREAU_LAST6M_"+"AMT_CREDIT_SUM_DEBT"+"_SUM")

    agg_bureau = agg_bureau.join(DAYS180_mean.join(DAYS180_max, "SK_ID_CURR").join(DAYS180_sum, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")


    val DAYS360 = tmp.where("DAYS_CREDIT >= -360")

    val DAYS360_mean = DAYS360.groupBy("SK_ID_CURR").mean("AMT_CREDIT_MAX_OVERDUE", "AMT_CREDIT_SUM_OVERDUE",
      "AMT_CREDIT_SUM_DEBT", "DEBT_PERCENTAGE", "DEBT_CREDIT_DIFF", "STATUS_0", "STATUS_12345")
      .withColumnRenamed("avg(AMT_CREDIT_MAX_OVERDUE)", "BUREAU_LAST12M_"+"AMT_CREDIT_MAX_OVERDUE"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM_OVERDUE)", "BUREAU_LAST12M_"+"AMT_CREDIT_SUM_OVERDUE"+"_MEAN")
      .withColumnRenamed("avg(AMT_CREDIT_SUM_DEBT)", "BUREAU_LAST12M_"+"AMT_CREDIT_SUM_DEBT"+"_MEAN")
      .withColumnRenamed("avg(DEBT_PERCENTAGE)", "BUREAU_LAST12M_"+"DEBT_PERCENTAGE"+"_MEAN")
      .withColumnRenamed("avg(DEBT_CREDIT_DIFF)", "BUREAU_LAST12M_"+"DEBT_CREDIT_DIFF"+"_MEAN")
      .withColumnRenamed("avg(STATUS_0)", "BUREAU_LAST12M_"+"STATUS_0"+"_MEAN")
      .withColumnRenamed("avg(STATUS_12345)", "BUREAU_LAST12M_"+"STATUS_12345"+"_MEAN")

    val DAYS360_max = DAYS360.groupBy("SK_ID_CURR").max("AMT_CREDIT_MAX_OVERDUE", "AMT_CREDIT_SUM")
      .withColumnRenamed("max(AMT_CREDIT_MAX_OVERDUE)", "BUREAU_LAST12M_"+"AMT_CREDIT_MAX_OVERDUE"+"_MAX")
      .withColumnRenamed("max(AMT_CREDIT_SUM)", "BUREAU_LAST12M_"+"AMT_CREDIT_SUM"+"_MAX")

    val DAYS360_sum = DAYS360.groupBy("SK_ID_CURR").sum("AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT")
      .withColumnRenamed("sum(AMT_CREDIT_SUM)", "BUREAU_LAST12M_"+"AMT_CREDIT_SUM"+"_SUM")
      .withColumnRenamed("sum(AMT_CREDIT_SUM_DEBT)", "BUREAU_LAST12M_"+"AMT_CREDIT_SUM_DEBT"+"_SUM")


    agg_bureau = agg_bureau.join(DAYS360_mean.join(DAYS360_max, "SK_ID_CURR").join(DAYS360_sum, "SK_ID_CURR"), Array("SK_ID_CURR"), "left")


    val lastFields = Array("SK_ID_CURR", "BUREAU_LAST_LOAN_MAX_OVERDUE").map(fieldName => StructField(fieldName, StringType, nullable = true))

    val lastSchema = StructType(lastFields)

    val last_tmp = tmp.select("SK_ID_CURR", "DAYS_CREDIT", "AMT_CREDIT_MAX_OVERDUE").rdd.map(row => (row(0), Array(row(1), row(2)))).reduceByKey(
        (x, y) => {
          if (Integer.parseInt(""+x(0)) > Integer.parseInt(""+y(0))){
            x
          }else{
            y
          }
        }
      ).map(x=>Row(x._1+"", x._2(1)+""))

    val lastDF = spark.createDataFrame(last_tmp, lastSchema).select($"SK_ID_CURR".cast(IntegerType), $"BUREAU_LAST_LOAN_MAX_OVERDUE".cast(DoubleType))


    agg_bureau = agg_bureau.join(lastDF, Array("SK_ID_CURR"), "left")


    agg_bureau = agg_bureau.withColumn("BUREAU_DEBT_OVER_CREDIT", $"BUREAU_AMT_CREDIT_SUM_DEBT_SUM" / $"BUREAU_AMT_CREDIT_SUM_SUM")
      .withColumn("BUREAU_ACTIVE_DEBT_OVER_CREDIT", $"BUREAU_ACTIVE_AMT_CREDIT_SUM_DEBT_SUM" / $"BUREAU_ACTIVE_AMT_CREDIT_SUM_SUM")

    agg_bureau.repartition(1).write.format("csv").option("header", "false").save("/user/spark/HC1001HC/"+ args(0))
  }
}
