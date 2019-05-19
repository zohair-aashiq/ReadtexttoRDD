import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._
import scala.util.control.Breaks._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import java.util.stream.IntStream
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection.mutable
object ReadFiles {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
          .appName("trial_dataset")
          .master("local[*]")
          .getOrCreate()
//        val IDENT_HEADER_PATH = args(0)
//        val SUB_HEADER_PATH = args(1)
//        val IDENT_LONGTEXT_PATH = args(2)
//        val IDENT_HEADER = spark.read.format("csv")
//          .option("header", "true")
//          .load(IDENT_HEADER_PATH)
//          .limit(10000)
        var sample_file = spark.read.format("csv")
          .option("header", "true")
          .load("src/main/resources/sample_file.csv")
          .limit(100)
        //initialize DF
        val dfRanges = makeRangeColumn(sample_file,10,spark)
        dfRanges.write.partitionBy("Ranges").parquet("src/main/resources/Output/IDENT_HEADER_parq.parquet")
        /*
        //val SUB_HEADER = spark.read.format("csv")
        //.option("header", "true")
         // .load(SUB_HEADER_PATH)
          //7.limit(100)
        //val IDENT_LONGTEXT = spark.read.format("csv")
          //.option("header", "true")
          .load(IDENT_LONGTEXT_PATH)
          .limit(100)

        val IDENT_SUB_HEADER = IDENT_HEADER.join(SUB_HEADER, (IDENT_HEADER("RECNO_ROOT") === SUB_HEADER("RECORD_NO")) && (IDENT_HEADER("SYSTEM") === SUB_HEADER("SYSTEM")), "left_outer").withColumnRenamed("_c0","RECORD_NO").dropDuplicateCols(IDENT_HEADER)

        val HEADER_LONGTEXT = IDENT_LONGTEXT.join(IDENT_HEADER, (IDENT_HEADER("RECORD_NO") === IDENT_LONGTEXT("REF_RECNRI")) && (IDENT_HEADER("SYSTEM") === IDENT_LONGTEXT("SYSTEM")), "left_outer").withColumnRenamed("_c0","RECORD_NO").dropDuplicateCols(IDENT_LONGTEXT)
        val numOfPartition = (IDENT_SUB_HEADER.count() / 1000).asInstanceOf[Int]
        println(IDENT_SUB_HEADER.show(10))
        println(HEADER_LONGTEXT.show(10))
        print(numOfPartition)
        IDENT_SUB_HEADER.repartitionByRange(numOfPartition,  IDENT_SUB_HEADER("RECORD_NO")).write.parquet("src/main/resources/Output/IDENT_SUB_HEADER.parquet")
        //HEADER_LONGTEXT.write.parquet("src/main/resources/Output/HEADER_LONGTEXT.parquet")

    }

    implicit class DataFrameOperations(df: DataFrame) {
        def dropDuplicateCols(rmvDF: DataFrame): DataFrame = {
            val cols = df.columns.groupBy(identity).mapValues(_.size).filter(_._2 > 1).keySet.toSeq

            @tailrec
            def deleteCol(df: DataFrame, cols: Seq[String]): DataFrame = {
                if (cols.size == 0) df else deleteCol(df.drop(rmvDF(cols.head)), cols.tail)
            }

            deleteCol(df, cols)
        }
    }
*/

    }
    def makeRangeColumn(dataFrame: DataFrame, rangeNum:Int, spark:SparkSession): DataFrame = {
        import spark.sqlContext.implicits._
        var cols = dataFrame.columns
        var dfWithFoo = cols.foldLeft(spark.emptyDataFrame)((a, b) => a.withColumn(b, lit("anyStringValue")))
        var initialDF = dfWithFoo.withColumn("Ranges", lit(null: String))
        val min_max = dataFrame.agg(min("RECORD_NO"), max("RECORD_NO")).head()
        var minDf = Integer.parseInt(min_max.getString(0))
        val maxDf = Integer.parseInt(min_max.getString(1))
        println(minDf)
        println(maxDf)
        minDf = 1
        val numOfPartition = (maxDf / rangeNum) + 1
        print("numOfpartition"+numOfPartition)
        var minRange = minDf
        var maxRange = minRange + 9
        println("minRange" + minRange)
        println("maxRange" + maxRange)
        for (x <- (1 to numOfPartition)) {
          val df = dataFrame.where(col("RECORD_NO").between(minRange, maxRange))
          val dfCount = df.count()
          val value: String = minRange + "-" + maxRange
          println("value" + value)
          //var dfRange = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
          //val dfRange = Seq(value).toDF("Range")
          val dfRange = Seq(value).toDF("Ranges")
          val arrayRange = IntStream.range(1, dfCount.toInt + 1).toArray
          var result = dfRange.withColumn("Dummy", explode(lit(arrayRange))).drop("Dummy")
          val v = Window.orderBy("Ranges")
          val x = Window.orderBy("RECORD_NO")
          val resultIndices = result.withColumn("index", row_number().over(v))
          val dataWithIndices = df.withColumn("index", row_number().over(x))
          val finalDf = dataWithIndices.join(resultIndices, Seq("index"), "outer").drop("index")
          initialDF = initialDF.union((finalDf)).toDF()
          minRange = maxRange + 1
          maxRange = maxRange + 10
        }
        initialDF
    }
}


