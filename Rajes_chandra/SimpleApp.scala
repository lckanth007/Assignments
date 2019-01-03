import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object SimpleApp {
  def main(args: Array[String]) {
    var Exitcode = 0
    val StartedTime = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)
    val log = ("Started", StartedTime)
    var LogList = List(log.toString().replace("(", "").replace(")", ""))

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/tmp")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .enableHiveSupport()
      .getOrCreate()

    try {
      val processingTime = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)
      val log1 = ("InProcessing", processingTime)
      LogList = log1.toString().replace("(", "").replace(")", "") :: LogList
      
            println("Proceeding with data file")
            val dataDF = spark.read.format("csv")
              //.schema(input6)
              .option("delimiter", "\u0007")
              .option("ignoreLeadingWhiteSpace", "True")
              .option("ignoreTrailingWhiteSpace", "True")
              .option("multiline", "True")
              .option("escape", "\u000D")
              .load(args(0)+"/"+args(1))

              //.load("/devlake/audit/ecomm/MP_LEGAL_ENTITY/pump_PROD4_MP_LEGAL_ENTITY_2018-09-21_14-26-13_00000_data.dsv.gz")
            println("Data file Found")
            //dataDF.write.format("csv").mode("overwrite").save("file:///home/azimukangda5500/Project-2/spark/SOURCE1/output/csv/")
            println("Proceeding with schema file")
            //val input = spark.sparkContext.textFile("/devlake/audit/ecomm/MP_LEGAL_ENTITYPROD4.MP_LEGAL_ENTITY.schema")
            val input = spark.sparkContext.textFile(args(0)+"/"+args(2))
            //input.collect.foreach(println)
            println("Schema file found")
      
            val input2 = input.map { x =>
              val w = x.split(":")
              val columnName = w(0).trim()
              val raw = w(1).trim()
              (columnName, raw)
            }
            val input3 = input2.map { x =>
              val x2 = x._2.replaceAll(";", "")
              (x._1, x2)
            }
            val input4 = input3.map { x =>
              val pattern1 = ".*int\\d{0,}".r
              val pattern2 = ".*string\\[.*\\]".r
              val pattern3 = ".*timestamp\\[.*\\]".r
              val raw1 = pattern1 replaceAllIn (x._2, "int")
              val raw2 = pattern2 replaceAllIn (raw1, "string")
              val raw3 = pattern3 replaceAllIn (raw2, "timestamp")
              val raw4 = x._1 + " " + raw3
              raw4
            }
            val input5 = "create table if not exists temp123 (" + input4.collect().toList.mkString(",") + ") stored as parquetfile"
            //Table created in hive default database
            //spark.sql("drop table if exists temp123")
            spark.sql(input5)
            dataDF.write.insertInto("temp123")
            //val hiveOut = spark.sql("select * from temp123")
            //hiveOut.coalesce(1).write.format("parquet").mode("overwrite").save("file:///home/azimukangda5500/Project-2/spark/SOURCE1/output/parquet/extract_date=" + java.time.LocalDateTime.now)
      
      val completedTime = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)
      val log2 = ("Completed", completedTime)
      LogList = log2.toString().replace("(", "").replace(")", "") :: LogList

    } catch {
      case e: Throwable =>
        println("File Not Found")
        val failedTime = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)
        Exitcode = 1
        val log3 = ("failed", failedTime)
        LogList = log3.toString().replace("(", "").replace(")", "") :: LogList
    } finally {
      //spark.sparkContext.parallelize(LogList).saveAsTextFile("/Users/cklekkala/IdeaProjects/untitled1/injecti.log")
      spark.sparkContext.parallelize(LogList).coalesce(1,false).saveAsTextFile(args(0)+"/SimpleApp.log")
      System.exit(Exitcode)
      spark.stop()

    }
  }
}
