package com.raoqiang.scala

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import com.raoqiang.scala.ObjectSet._
object ExtSources {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Ext Sources").getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    // 读取 application_train文件，需要去掉TARGET列
    val applicationDFCsv1 = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("/user/spark/data/HC1000HC"+ args(0) +".csv")
      .drop($"TARGET")
      .where("CODE_GENDER!='XNA'")     // 去掉CODE_GENDER列值为XNA的行，不参与统计
      .where("AMT_INCOME_TOTAL < 20000000")  // 去掉 AMT_INCOME_TOTAL 列值小于20,000,000的行，不参与统计
      .na.fill(value=365243,cols=Array("DAYS_EMPLOYED")) // 填充DAYS_EMPLOYED列为null的栏位

    // 读取application_test文件
    val applicationDFCsv2 = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("/user/spark/data/HC1008HC"+ args(0) +".csv")
      .where("CODE_GENDER!='XNA'")
      .where("AMT_INCOME_TOTAL < 20000000")
      .na.fill(value=365243,cols=Array("DAYS_EMPLOYED"))

    // 合并train 和 test文件数据
    val applicationDFCsv = applicationDFCsv1.union(applicationDFCsv2).sort("SK_ID_CURR")


    // ['ORGANIZATION_TYPE', 'NAME_EDUCATION_TYPE', 'OCCUPATION_TYPE', 'AGE_RANGE', 'CODE_GENDER']
    // 筛选聚合栏位
    val a = applicationDFCsv.select($"SK_ID_CURR",  $"EXT_SOURCE_1".cast(DoubleType),
      $"EXT_SOURCE_2".cast(DoubleType), $"EXT_SOURCE_3".cast(DoubleType), $"DAYS_BIRTH",
      $"ORGANIZATION_TYPE", $"NAME_EDUCATION_TYPE", $"OCCUPATION_TYPE", $"CODE_GENDER", $"AMT_INCOME_TOTAL",
      $"AMT_CREDIT", $"AMT_ANNUITY"
    )

    // 数据处理
    val ext =  a.rdd.map(row => Row(
        row(0)+"",
        extMin(row(1)+"", extMin(row(3)+"", row(2)+"")+"")+"", // 最小值
        extMax(row(1)+"", extMax(row(3)+"", row(2)+"")+"")+"", // 最大值
        extMean(row(1)+"",row(2)+"",row(3)+"")+"", // 均值
        extPord(row(1)+"",row(2)+"",row(3)+"")+"", // 乘积
        extWeighted(row(1)+"",row(2)+"",row(3)+"")+"", // 加权值
        extNanmedian(row(1)+"",row(2)+"",row(3)+"")+"", // 中位数
        extVar(row(1)+"",row(2)+"",row(3)+"")+"", // 方差
        birth2Label(""+row(4))+"", // DAYS_BIRTH 数值化
        creditToAnnuityRatio(row(10)+"", row(11)+"")+"" // 栏位计算
      ))


    // schema 定义
    val schemaString = "SK_ID_CURR EXT_SOURCES_MIN EXT_SOURCES_MAX EXT_SOURCES_MEAN EXT_SOURCES_PROD EXT_SOURCES_WEIGHTED EXT_SOURCES_NANMEDIAN EXT_SOURCES_VAR AGE_RANGE CREDIT_TO_ANNUITY_RATIO"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
      // 字段类型，字段名称判断是不是为空

    val schema = StructType(fields)

    // rdd 转换成 DataFrame
    val extDF = spark.createDataFrame(ext, schema)
//      .show(5)
//    extDF.show(40)

    // 注册用户自定义函数
    spark.udf.register("myStd", MyStd)

    spark.udf.register("myMedian", MyMedian)


    // 根据栏位聚合，
    val groups = extDF.join(a,"SK_ID_CURR").select($"ORGANIZATION_TYPE", $"NAME_EDUCATION_TYPE",
      $"OCCUPATION_TYPE", $"CODE_GENDER", $"AMT_INCOME_TOTAL".cast(DoubleType), $"AMT_CREDIT".cast(DoubleType),
      $"EXT_SOURCES_MEAN".cast(DoubleType), $"AMT_ANNUITY".cast(DoubleType),
      $"CREDIT_TO_ANNUITY_RATIO".cast(DoubleType), $"AGE_RANGE").
      groupBy($"ORGANIZATION_TYPE", $"NAME_EDUCATION_TYPE", $"OCCUPATION_TYPE", $"CODE_GENDER", $"AGE_RANGE")


    val median_agg =  groups.
      agg(Map("EXT_SOURCES_MEAN" -> "myMedian")).
      withColumnRenamed("mymedian(EXT_SOURCES_MEAN)", "GROUP_EXT_SOURCES_MEDIAN")

    // 聚合后获取集合栏位的标准差，如果将均值和标准差放在一起计算，将会出现Map 的key值一致导致只计算后面出现的函数
    val std_agg =  groups.
      agg(Map("AMT_INCOME_TOTAL" -> "myStd", "EXT_SOURCES_MEAN" -> "myStd", "AMT_ANNUITY" -> "myStd",
        "CREDIT_TO_ANNUITY_RATIO" -> "myStd")).
      withColumnRenamed("myStd(AMT_INCOME_TOTAL)", "GROUP_INCOME_STD").
      withColumnRenamed("myStd(EXT_SOURCES_MEAN)", "GROUP_EXT_SOURCES_STD").
      withColumnRenamed("myStd(AMT_ANNUITY)", "GROUP_ANNUITY_STD").
      withColumnRenamed("myStd(CREDIT_TO_ANNUITY_RATIO)", "GROUP_CREDIT_TO_ANNUITY_STD")




    // 聚合后获取集合栏位的均值
    val mean_agg =  groups.
      mean("AMT_INCOME_TOTAL", "AMT_CREDIT", "AMT_ANNUITY", "CREDIT_TO_ANNUITY_RATIO").
      withColumnRenamed("avg(AMT_INCOME_TOTAL)", "GROUP_INCOME_MEAN").
      withColumnRenamed("avg(AMT_CREDIT)", "GROUP_CREDIT_MEAN").
      withColumnRenamed("avg(AMT_ANNUITY)", "GROUP_ANNUITY_MEAN").
      withColumnRenamed("avg(CREDIT_TO_ANNUITY_RATIO)", "GROUP_CREDIT_TO_ANNUITY_MEAN")


    // 合并 DataFrame
    val std_mean = median_agg.join(mean_agg, Seq("ORGANIZATION_TYPE", "NAME_EDUCATION_TYPE", "OCCUPATION_TYPE", "CODE_GENDER", "AGE_RANGE")).join(std_agg, Seq("ORGANIZATION_TYPE", "NAME_EDUCATION_TYPE", "OCCUPATION_TYPE", "CODE_GENDER", "AGE_RANGE"))

    std_mean.repartition(1).write.format("csv").option("header", "false").save("/user/spark/HC1000HC/"+ args(0))

  }



}
