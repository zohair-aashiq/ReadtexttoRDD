import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Dataset
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import java.util.stream.IntStream

import scala.annotation.tailrec
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
//        dfRanges.write.partitionBy("Range").parquet("src/main/resources/Output/IDENT_HEADER_parq.parquet")
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
    def makeRangeColumn(dataFrame: DataFrame, rangeNum:Int, spark:SparkSession): DataFrame ={
        import spark.sqlContext.implicits._
        val schema = StructType(
            StructField("Range", StringType, true) :: Nil)
        val metaHashMap: mutable.HashMap[String,String] = mutable.HashMap.empty[String,String]
        var initialDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
        val y = Window.orderBy("RECORD_NO")
        val dfWithId = dataFrame.withColumn("index", row_number().over(y))
        val numOfPartition = (dfWithId.count / rangeNum).asInstanceOf[Int]
        var minRange = 1
        var maxRange = rangeNum
        for(x <-(1 to numOfPartition)) {
            val df = dfWithId.where(col("index").between(minRange, maxRange))
            val dfCount = df.count()
            val minimum = Integer.parseInt(df.agg(min("RECORD_NO")).first().getString(0))
            val maximum = Integer.parseInt(df.agg(max("RECORD_NO")).first().getString(0))
            val value:String = minimum + "-" + maximum
            val dfRange = Seq(value).toDF("Range")
            val array = IntStream.range(minRange, maxRange+1).toArray
            val result = dfRange.withColumn("Dummy", explode(lit(array))).drop("Dummy")
            initialDF = initialDF.union((result)).toDF()
            minRange = maxRange + 1
            maxRange = maxRange + 10
            metaHashMap.getOrElseUpdate(minimum.toString, value)

        }
        println("This is a meta Hashmap"+metaHashMap.foreach(println))
        val metaDf = metaHashMap.toSeq.toDF("MinimumValue", "Range")
        metaDf
          .coalesce(1)
          .write.format("com.databricks.spark.csv")
          .option("header", "true")
          .save("src/main/resources/Output/parquetFiles_metaTable.csv")
        val w = Window.orderBy("Range")
        val resultDf = initialDF.withColumn("index", row_number().over(w))
        val finalDf = dfWithId.join(resultDf, Seq("index"),"outer")

        finalDf
    }
}


