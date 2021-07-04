import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.typesafe.config.{Config, ConfigFactory}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import gkFunctions.read_schema
object DailyDataIngestAndRefine {
def main(args: Array[String]) ={
  val spark = SparkSession.builder.appName("DailyDataIngestAndRefine").master("local[*]").getOrCreate()
  val sc = spark.sparkContext

  //Reading landing data from Config File

  val gkconfig : Config = ConfigFactory.load("application.conf")
  val inputLocation = gkconfig.getString("paths.inputLocation")
  val outputLocation = gkconfig.getString("paths.outputLocation")
  import spark.implicits._
 val landingFileSchemaFromFile = gkconfig.getString("schema.landingFileSchema")
 val holdFileSchemaFromFile = gkconfig.getString("schema.holdFileSchema")
  val landingFileSchema = read_schema(landingFileSchemaFromFile)
  val  holdFileSchema = read_schema(holdFileSchemaFromFile)
  /* StructType(List(StructField("Sale_ID",StringType,true),
                                         StructField("Product_ID",StringType,true),
                                         StructField("Quantity_Sold",IntegerType,true),
                                         StructField("Vendor_ID",StringType,true),
                                         StructField("Sale_Date",TimestampType,true),
                                         StructField("Sale_Amount",DoubleType,true),
                                         StructField("Sale_Currency",StringType,true)))
  */

  //Handling Dates
   //val dateToday = LocalDate.now
   //val yesterDate = dateToday.minusDays(1)

 //   val currentDayZoneSuffix = "_" + dateToday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
 // val yesterDayZoneSuffix = "_" + yesterDate.format(DateTimeFormatter.ofPattern("ddMMyyyy"))

  val yesterDayZoneSuffix = "_18072020"
  val currentDayZoneSuffix = "_19072020"

  val landingFileDF = spark.read
                       .schema(landingFileSchema)
                       .option("delimiter", "|")
                       .csv(inputLocation + "Sales_Landing\\SalesDump" +  currentDayZoneSuffix)
  landingFileDF.createOrReplaceTempView("landingFileDF")
//Checking if updates were received on any previous data
     val previousHoldDF = spark.read
                        .schema(holdFileSchema)
                        .option("delimiter", "|")
                        .option("header","true")
                        .csv(outputLocation + "Hold/HoldData" +  yesterDayZoneSuffix)
  previousHoldDF.createOrReplaceTempView("previousHoldDF")

   val refreshedLandingData = spark.sql("select a.SALE_ID,a.Product_ID," +
     "CASE" +
     " WHEN (a.Quantity_Sold IS NULL ) THEN b.Quantity_Sold " +
     "else a.Quantity_Sold " +
     "end as Quantity_Sold, " +
     "CASE When(a.Vendor_ID is null) then b.Vendor_ID " +
     "else a.Vendor_ID " +
     "end as Vendor_ID, " +
     "a.Sale_Date, a.Sale_Amount, a.Sale_Currency " +
     "  from landingFileDF a " +
     "left outer join previousHoldDF b on a.SALE_ID=b.SALE_ID "

   )
  refreshedLandingData.createOrReplaceTempView("refreshedLandingData")
  val validLandingData  = refreshedLandingData.filter($"Quantity_Sold".isNotNull && $"Vendor_ID".isNotNull)
 validLandingData.createOrReplaceTempView("validLandingData")

  val releasedFromHold = spark.sql("select vd.Sale_ID from validLandingData vd inner join previousHoldDF phd  " +
                                   " on vd.Sale_ID=phd.Sale_ID")
  releasedFromHold.createOrReplaceTempView("releasedFromHold")

  val notReleasedFromHold = spark.sql("select * from previousHoldDF " +
    " where Sale_ID not in (select Sale_ID from releasedFromHold )")


  val invalidLandingData  = refreshedLandingData.filter($"Quantity_Sold".isNull || $"Vendor_ID".isNull)
                                          .withColumn("Hold_Reason",when($"Quantity_Sold".isNull, "Qty is Missing")
                                            .otherwise(when($"Vendor_ID".isNull, "V_ID Missing"))
                                          ).union(notReleasedFromHold)

  validLandingData.write
    .mode("overwrite")
    .option("delimiter","|")
    .option("header","true")
    .csv(outputLocation + "Valid/ValidData"+ currentDayZoneSuffix)

  invalidLandingData.write
    .mode("overwrite")
    .option("delimiter","|")
    .option("header","true")
    .csv(outputLocation + "Hold/HoldData"+ currentDayZoneSuffix)





}

}
