package com.raoqiang.scala

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object ObjectSet {



  object Nunique extends UserDefinedAggregateFunction{
    override def inputSchema: StructType = StructType(StructField("inputColumn", StringType) :: Nil)

    override def bufferSchema: StructType = {
      StructType(StructField("str", StringType) :: StructField("count", LongType) :: Nil)
    }

    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      // 存储字符串
      buffer(0) = ""
      // 参与计算元素数量
      buffer(1) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0) && !buffer.getString(0).split("!&").contains(input.getString(0)+"")) {
        // 更新求和
        buffer(0) = buffer.getString(0) +"!&"+ input.getString(0)
        // 更新求平方和
        buffer(1) = buffer.getLong(1) + 1
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      // 聚合 求和
      buffer1(0) = buffer1.getString(0) +"!&"+ buffer2.getString(0)
      // 聚合 求和
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    override def evaluate(buffer: Row): Double = {
      // 标准差
      //      (buffer.getDouble(1) * (buffer.getLong(2)-1) - buffer.getDouble(0) * buffer.getDouble(0))/((buffer.getLong(2)-1) * (buffer.getLong(2)-1))
      //      buffer.getDouble(1) / buffer.getLong(2) - (buffer.getDouble(0) * buffer.getDouble(0) / (buffer.getLong(2) * buffer.getLong(2)))
      // 方差，同python pandas 的 无偏差方差 分母为：n-1
      (""+buffer(0)).split("!&").distinct.dropWhile(x=>x.equals("")).length
    }
  }

  object MyVar extends UserDefinedAggregateFunction{
    override def inputSchema: StructType = StructType(StructField("inputColumn", DoubleType) :: Nil)

    override def bufferSchema: StructType = {
      StructType(StructField("mean", DoubleType) :: StructField("sqrt", DoubleType) :: StructField("count", LongType) :: Nil)
    }

    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      // 求和 获取均值
      buffer(0) = 0.0
      // 获取平方和
      buffer(1) = 0.0
      // 参与计算元素数量
      buffer(2) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        // 更新求和
        buffer(0) = buffer.getDouble(0) + input.getDouble(0)
        // 更新求平方和
        buffer(1) = buffer.getDouble(1) + input.getDouble(0) * input.getDouble(0)
        // 更新数量
        buffer(2) = buffer.getLong(2) + 1
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      // 聚合 求和
      buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
      // 聚合 求和
      buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1)
      // 聚合 相加
      buffer1(2) = buffer1.getLong(2) + buffer2.getLong(2)
    }

    override def evaluate(buffer: Row): Double = {
      // 标准差
      //      (buffer.getDouble(1) * (buffer.getLong(2)-1) - buffer.getDouble(0) * buffer.getDouble(0))/((buffer.getLong(2)-1) * (buffer.getLong(2)-1))
      //      buffer.getDouble(1) / buffer.getLong(2) - (buffer.getDouble(0) * buffer.getDouble(0) / (buffer.getLong(2) * buffer.getLong(2)))
      // 方差，同python pandas 的 无偏差方差 分母为：n-1
      buffer.getDouble(1) / (buffer.getLong(2)-1) - (buffer.getDouble(0) * buffer.getDouble(0)) / ((buffer.getLong(2)-1) * buffer.getLong(2))
    }
  }

  def one_hot(prefix:String, types:Array[String], df:DataFrame): DataFrame ={
    //    import spark.implicits._
    var tmp = df
    types.foreach(x =>
      tmp = tmp.withColumn(prefix + "_" + x, (tmp.col(prefix)===x).cast(IntegerType))
    )
    tmp
  }

  //EXT_SOURCES_VAR 计算方差，
  def extVar(str1:String, str2:String, str3:String): Any ={
    if (isNotDouble(str1) && isNotDouble(str2) && isNotDouble(str3)){
      null
    }else{
      var sqr: java.lang.Double = 0.0
      val mean = java.lang.Double.parseDouble(""+extMean(str1, str2, str3))
      var c:Integer = 0
      // 判断，空值比参与计算
      if(!isNotDouble(str1)){
        sqr += (java.lang.Double.parseDouble(""+str1)-mean) * (java.lang.Double.parseDouble(""+str1)-mean)
        c += 1
      }
      if(!isNotDouble(str2)){
        sqr += (java.lang.Double.parseDouble(""+str2)-mean) * (java.lang.Double.parseDouble(""+str2)-mean)
        c += 1
      }
      if(!isNotDouble(str3)){
        sqr += (java.lang.Double.parseDouble(""+str3)-mean) * (java.lang.Double.parseDouble(""+str3)-mean)
        c += 1
      }
      if(c == 0){
        null
      } else{
        sqr / c
      }
    }
  }

  //EXT_SOURCES_NANMEDIAN 计算中位数
  def extNanmedian(str1:String, str2:String, str3:String): Any ={
    if (isNotDouble(str1) && isNotDouble(str2) && isNotDouble(str3)){
      null
    }else{
      var d: java.lang.Double = 0.0
      var c:Integer = 0
      // 判断，空值不参与计算
      if(!isNotDouble(str1)){
        d += java.lang.Double.parseDouble(""+str1)
        c += 1
      }
      if(!isNotDouble(str2)){
        d += java.lang.Double.parseDouble(""+str2)
        c += 1
      }
      if(!isNotDouble(str3)){
        d += java.lang.Double.parseDouble(""+str3)
        c += 1
      }
      if(c == 0){
        null
      }else if(c == 2 || c == 1){
        // c=2 取平均值，c=1 取d
        d/c
      }else{
        // c=3 取中间那个数
        d - java.lang.Double.parseDouble(extMax(str1, extMax(str2, str3)+"")+"") - java.lang.Double.parseDouble(extMin(str1, extMin(str2, str3)+"")+"")
      }
    }
  }

  // CreditToAnnuityRatio 栏位计算 两数相除
  def creditToAnnuityRatio(str1:String, str2:String): Any = {
    if (isNotDouble(str1) || isNotDouble(str2)){
      null
    }else{
      java.lang.Double.parseDouble(""+str1) / java.lang.Double.parseDouble(""+str2)
    }
  }


  //  EXT_SOURCES_PROD
  //  EXT_SOURCES_WEIGHTED 求积
  def extPord(str1:String, str2:String, str3:String): Any = {
    if (isNotDouble(str1) || isNotDouble(str2) || isNotDouble(str3)) {
      null
    }else{
      java.lang.Double.parseDouble(""+str1) *
        java.lang.Double.parseDouble(""+str2) *
        java.lang.Double.parseDouble(""+str3)
    }
  }

  // 加权和
  def extWeighted(str1:String, str2:String, str3:String): Any = {
    if (isNotDouble(str1) || isNotDouble(str2) || isNotDouble(str3)) {
      null
    }else{
      java.lang.Double.parseDouble(""+str1) * 2 +
        java.lang.Double.parseDouble(""+str2) * 1 +
        java.lang.Double.parseDouble(""+str3) * 3
    }
  }


  // 最小值
  def extMin(str1:String, str2:String): Any ={
    if (isNotDouble(str1) && isNotDouble(str2)){
      null
    }else if(isNotDouble(str1)){
      str2
    }else if(isNotDouble(str2)){
      str1
    }else{
      Math.min(java.lang.Double.parseDouble(""+str1), java.lang.Double.parseDouble(""+str2))
    }
  }

  // 最大值
  def extMax(str1:String, str2:String): Any ={
    if (isNotDouble(str1) && isNotDouble(str2)){
      null
    }else if(isNotDouble(str1)){
      str2
    }else if(isNotDouble(str2)){
      str1
    }else{
      Math.max(java.lang.Double.parseDouble(""+str1), java.lang.Double.parseDouble(""+str2))
    }
  }

  // 均值计算
  def extMean(str1:String, str2:String, str3:String): Any ={
    if (isNotDouble(str1) && isNotDouble(str2) && isNotDouble(str3)){
      null
    }else{
      var d: java.lang.Double = 0.0
      var c:Integer = 0
      if(!isNotDouble(str1)){
        d += java.lang.Double.parseDouble(""+str1)
        c += 1
      }
      if(!isNotDouble(str2)){
        d += java.lang.Double.parseDouble(""+str2)
        c += 1
      }
      if(!isNotDouble(str3)){
        d += java.lang.Double.parseDouble(""+str3)
        c += 1
      }
      d/c
    }
  }

  // 是否参与计算判断
  def isNotDouble(str:String): Boolean={

    if(str == null || str.trim.equals("") || str.trim.equals("null")){
      true
    }else{
      false
    }
  }

  // BIRTH_DAYS 转 整数
  def birth2Label(str:String): Int={
    val ageYears = -Integer.parseInt(str) / 365
    if (ageYears < 27)  1
    else if (ageYears < 40)  2
    else if (ageYears < 50)  3
    else if (ageYears < 65)  4
    else if (ageYears < 99)  5
    else 0
  }

  //
  // 自定义聚合函数 求标准差 sqrt(E(X2) - (u2))
  object MyStd extends UserDefinedAggregateFunction{
    override def inputSchema: StructType = StructType(StructField("inputColumn", DoubleType) :: Nil)

    override def bufferSchema: StructType = {
      StructType(StructField("mean", DoubleType) :: StructField("sqrt", DoubleType) :: StructField("count", LongType) :: Nil)
    }

    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      // 求和 获取均值
      buffer(0) = 0.0
      // 获取平方和
      buffer(1) = 0.0
      // 参与计算元素数量
      buffer(2) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        // 更新求和
        buffer(0) = buffer.getDouble(0) + input.getDouble(0)
        // 更新求平方和
        buffer(1) = buffer.getDouble(1) + input.getDouble(0) * input.getDouble(0)
        // 更新数量
        buffer(2) = buffer.getLong(2) + 1
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      // 聚合 求和
      buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
      // 聚合 求和
      buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1)
      // 聚合 相加
      buffer1(2) = buffer1.getLong(2) + buffer2.getLong(2)
    }

    override def evaluate(buffer: Row): Double = {
      // 标准差
      math.sqrt(buffer.getDouble(1) / (buffer.getLong(2)-1) - (buffer.getDouble(0) * buffer.getDouble(0)) / ((buffer.getLong(2)-1) * buffer.getLong(2)))
    }
  }

  // 自定义聚合函数 求标准差 sqrt(E(X2) - (u2))
  object MyMedian extends UserDefinedAggregateFunction{
    override def inputSchema: StructType = StructType(StructField("inputColumn", DoubleType) :: Nil)

    override def bufferSchema: StructType = {
      StructType(StructField("arrays", StringType) :: StructField("count", LongType) :: Nil)
    }

    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      // 求和 获取均值
      buffer(0) = ""
      // 参与计算元素数量
      buffer(1) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        // 更新求和
        buffer(0) = buffer.getString(0) +"!&"+ input.getDouble(0)
        // 更新数量
        buffer(1) = buffer.getLong(1) + 1
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      // 聚合 求和
      buffer1(0) = buffer1.getString(0) + buffer2.getString(0)
      // 聚合 相加
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    override def evaluate(buffer: Row): Double = {
      // 标准差
      val a = buffer.getString(0).split("!&").filter(x=>x!=null && !x.equals("")).map(x => java.lang.Double.parseDouble(x)).sortWith((x,y) => x > y)

      if(a.length==0){
        0.0
      }else if(a.length==1){
        a(0)
      }else if(a.length /2 == 0){
        a(a.length/2) + a(a.length/2-1)
      }else{
        a(a.length/2)
      }

    }
  }

  object Top3 extends UserDefinedAggregateFunction{
    override def inputSchema: StructType = StructType(StructField("inputColumn", DoubleType) :: Nil)

    override def bufferSchema: StructType = {
      StructType(StructField("mean", DoubleType) :: StructField("sqrt", DoubleType) :: StructField("count", DoubleType) :: StructField("count1", DoubleType) :: Nil)
    }

    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      // 求和 获取均值
      buffer(0) = Double.NegativeInfinity
      buffer(1) = Double.NegativeInfinity
      // 获取平方和
      buffer(2) = Double.NegativeInfinity
      // 参与计算元素数量
      buffer(3) = 0.0
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0) && input.getDouble(0) > buffer.getDouble(2)) {
        if(input.getDouble(0) > buffer.getDouble(0)){
          buffer(0) = input.getDouble(0)
          buffer(1) = buffer.getDouble(0)
          buffer(2) = buffer.getDouble(1)

        }else if(input.getDouble(0) > buffer.getDouble(1)){
          buffer(1) = input.getDouble(0)
          buffer(2) = buffer.getDouble(1)
        }else{
          buffer(2) = input.getDouble(0)
        }
        buffer(3) =  buffer.getDouble(3) + 1
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val tmp = Array(buffer1.getDouble(0), buffer1.getDouble(1), buffer1.getDouble(2),
        buffer2.getDouble(0), buffer2.getDouble(1), buffer2.getDouble(2)).sortWith((x,y)=>x>y)
      buffer1(0) = tmp(0)
      buffer1(1) = tmp(1)
      buffer1(2) = tmp(2)
      buffer1(3) = buffer1.getDouble(3) + buffer2.getDouble(3)

    }

    override def evaluate(buffer: Row): Double = {
      //      val count = buffer.getLong(3) > 3 ? 3 :  buffer.getLong(3)
      if (buffer.getDouble(3) == 0.0){
        0.0
      }else if(buffer.getDouble(3) >= 3.0){
        (buffer.getDouble(0) + buffer.getDouble(1) + buffer.getDouble(2))/3
      }else if(buffer.getDouble(3) == 2.0){
        (buffer.getDouble(0) + buffer.getDouble(1))/2
      }else{
        buffer.getDouble(0)
      }
    }
  }

}
