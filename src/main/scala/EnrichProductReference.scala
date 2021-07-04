import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField,StructType,IntegerType,StringType,TimestampType,DoubleType}
import com.typesafe.config.{Config, ConfigFactory}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import gkFunctions.read_schema
object EnrichProductReference {
  def main(args : Array[String]) = {
    val spark = SparkSession.builder.appName("EnrichProductReference").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    //Reading landing data from Config File

    val gkconfig : Config = ConfigFactory.load("application.conf")
    val inputLocation = gkconfig.getString("paths.inputLocation")
    val outputLocation = gkconfig.getString("paths.outputLocation")
    import spark.implicits._
    //Reading Vaiid Data

    val validFileSchema = StructType(List(StructField("Sale_ID",StringType,true),
      StructField("Product_ID",StringType,true),
      StructField("Quantity_Sold",IntegerType,true),
      StructField("Vendor_ID",StringType,true),
      StructField("Sale_Date",TimestampType,true),
      StructField("Sale_Amount",DoubleType,true),
      StructField("Sale_Currency",StringType,true)))

    val productPriceReferenceSchema =  StructType(List(
      StructField("Product_ID",StringType,true),
      StructField("Product_Name",StringType,true),
      StructField("Product_Price",IntegerType,true),
      StructField("Product_Price_Currency",StringType,true),
      StructField("Product_updated_Date",TimestampType,true)
    ))

    //Handling Dates
    //val dateToday = LocalDate.now
    //val yesterDate = dateToday.minusDays(1)

    //   val currentDayZoneSuffix = "_" + dateToday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    // val yesterDayZoneSuffix = "_" + yesterDate.format(DateTimeFormatter.ofPattern("ddMMyyyy"))

    val yesterDayZoneSuffix = "_18072020"
    val currentDayZoneSuffix = "_19072020"

    val validDataDF = spark.read
      .schema(validFileSchema)
      .option("delimiter", "|")
      .option("header","true")
      .csv(outputLocation + "Valid\\ValidData" +  currentDayZoneSuffix)
    validDataDF.createOrReplaceTempView("validDataDF")

    //Reading Product Reference
    val productPriceReferenceDF = spark.read
      .schema(productPriceReferenceSchema)
      .option("delimiter","|")
      .option("header","true")
      .csv(inputLocation + "Products")
    productPriceReferenceDF.createOrReplaceTempView("productPriceReferenceDF")

    val productEnrichedDF = spark.sql("select a.Sale_ID, a.Product_ID, b.Product_Name, a.Quantity_Sold, " +
      "a.Vendor_ID, a.Sale_Date, " +
      "b.Product_Price * a.Quantity_Sold as Sale_Amount, " +
      "a.Sale_Currency " +
      "from " +
      "validDataDF a INNER JOIN productPriceReferenceDF b on a.Product_ID=b.Product_ID")

    productEnrichedDF.write
      .option("header","true")
      .option("delimiter","|")
      .mode("overwrite")
      .csv(outputLocation + "Enriched/SaleAmountEnrichment/SaleAmountEnriched" + currentDayZoneSuffix)
  }

}
