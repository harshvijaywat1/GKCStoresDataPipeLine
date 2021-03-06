import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType, TimestampType}
import com.typesafe.config.{Config, ConfigFactory}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import gkFunctions.read_schema
object VendorEnrichment {
def main(args :Array[String]): Unit ={
  val spark = SparkSession.builder.appName("VendorEnrichment").master("local[*]").getOrCreate()
  val sc = spark.sparkContext

  //Reading landing data from Config File

  val gkconfig : Config = ConfigFactory.load("application.conf")
  val inputLocation = gkconfig.getString("paths.inputLocation")
  val outputLocation = gkconfig.getString("paths.outputLocation")

  //Handling Dates
  //val dateToday = LocalDate.now
  //val yesterDate = dateToday.minusDays(1)

  //   val currentDayZoneSuffix = "_" + dateToday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
  // val yesterDayZoneSuffix = "_" + yesterDate.format(DateTimeFormatter.ofPattern("ddMMyyyy"))

  val yesterDayZoneSuffix = "_18072020"
  val currentDayZoneSuffix = "_19072020"

  val productEnrichedInputSchema = StructType(List(
    StructField("Sale_ID",StringType,true),
    StructField("Product_ID",StringType,true),
    StructField("Product_Name",StringType,true),
    StructField("Quantity_Sold",IntegerType,true),
    StructField("Vendor_ID",StringType,true),
    StructField("Sale_Date",TimestampType,true),
    StructField("Sale_Amount",DoubleType,true),
    StructField("Sale_Currency",StringType,true)
  ))

 val vendorReferenceSchema = StructType(List(
   StructField("Vendor_ID",StringType,true),
   StructField("Vendor_Name",StringType,true),
   StructField("Vendor_Add_Street",StringType,true),
   StructField("Vendor_Add_City",StringType,true),
   StructField("Vendor_Add_State",StringType,true),
   StructField("Vendor_Add_Country",StringType,true),
   StructField("Vendor_Add_Zip",StringType,true),
   StructField("Vendor_Updated_Date",TimestampType,true)
 ))

  val usdReferenceSchema = StructType(List(
    StructField("Currency",StringType,true),
    StructField("Currency_Code",StringType,true),
    StructField("Exchange_Rate",FloatType,true),
    StructField("Currency_Updated_Date",TimestampType,true)
  ))


  //Reading the required zones

  val productEnrichedDF = spark.read
                         .schema(productEnrichedInputSchema)
                         .option("delimiter","|")
                         .option("header","true")
    .csv(outputLocation + "Enriched/SaleAmountEnrichment/SaleAmountEnriched" + currentDayZoneSuffix)
 productEnrichedDF.createOrReplaceTempView("productEnrichedDF")

  val usdReferenceDF = spark.read
                      .schema(usdReferenceSchema)
                      .option("delimiter","|")
                      .option("header","false")
                      .csv(inputLocation + "USD_Rates")
  usdReferenceDF.createOrReplaceTempView("usdReferenceDF")

  val vendorReferenceDF = spark.read
                         .schema(vendorReferenceSchema)
    .option("delimiter","|")
    .option("header","false")
    .csv(inputLocation + "Vendors")
  vendorReferenceDF.createOrReplaceTempView("vendorReferenceDF")

  val vendorEnrichedDF = spark.sql("select a.*, b.Vendor_Name " +
    " from productEnrichedDF a inner join vendorReferenceDF b on a.Vendor_ID=b.Vendor_ID")
  vendorEnrichedDF.createOrReplaceTempView("vendorEnrichedDF")

  val usdEnriched = spark.sql("select *, round((a.Sale_Amount / b.Exchange_Rate),2) as Amount_USD" +
    " from vendorEnrichedDF a inner join usdReferenceDF b on a.Sale_Currency=b.Currency_Code")
 usdEnriched.createOrReplaceTempView("usdEnriched")
  usdEnriched.write
    .mode("overwrite")
    .option("delimiter","|")
    .option("header","true")
    .csv(outputLocation + "Enriched/Vendor_USD_Enriched/Vendor_USD_Enriched" + currentDayZoneSuffix)

  val reportDF = spark.sql("select Sale_ID, Product_ID, Product_Name, Quantity_Sold, " +
    "Vendor_ID, Sale_Date, Sale_Amount, Sale_Currency, Vendor_Name, Amount_USD from usdEnriched ")

  //Mysql Connectivity

   /*reportDF.write.format("jdbc")
                .options(Map(
                  "url"-> "jdbc:mysql://127.0.0.1:3306/gkcstoredb",
                  "driver" -> "com.mysql.jdbc.Driver",
                  "dbtable" -> "finalsales",
                  "user" -> "root",
                  "password" -> "root"

                )).mode("append")
                 .save()
            */
}
}
