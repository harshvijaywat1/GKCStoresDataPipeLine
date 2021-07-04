import org.apache.spark.sql.types._;
object gkFunctions {
 def read_schema(schema_arg :String ) = {
  var sch : StructType = new StructType();
  val split_values = schema_arg.split(",").toList

  val d_types = Map(
                  "StringType" -> StringType,
                  "IntegerType" -> IntegerType,
                  "DoubleType" -> DoubleType,
                  "TimestampType"-> TimestampType

                   )


  for(i <- split_values)
   { val columnVal = i.split(" ").toList
    sch = sch.add(columnVal(0), d_types(columnVal(1)),true)

   }

  sch
 }


}
