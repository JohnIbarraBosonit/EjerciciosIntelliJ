import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Programa {

  val file_in = "C:\\Users\\johnerik.ibarra\\Desktop\\Archivos\\Pokemon.csv"
  val file_out = "C:\\Users\\johnerik.ibarra\\Desktop\\Archivos\\Pokemon_out.csv"
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Word Count")
      .getOrCreate()

    val df = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file_in)

    val df_nueva_columna = df.withColumn("Suma", (col("Attack")+col("Defense")+col("Speed"))/3)

    df_nueva_columna.show()

    //df_nueva_columna.write.csv(file_out)
  }
}
