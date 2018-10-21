package weekfour

import java.nio.file.Paths

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object timeusage {

  val spark = SparkSession.builder().appName("Week4").master("local").getOrCreate()

  def main(args: Array[String]): Unit = {
    timeUsageByLifePeriod()
  }

  def timeUsageByLifePeriod(): Unit = {
    val (columns, initDf) = read("/timeusage/atussum.csv")
    //passing all column names and dividing those column names into three required categories
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    //passing required three category column names and Dataframe, and get the required Dataframe according to problem statement.
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val finalDf = timeUsageGrouped(summaryDf)
    finalDf.show()
  }

  def read(resource: String): (List[String], DataFrame) = {
    val rdd = spark.sparkContext.textFile(fsPath(resource))

    val headerColumns = rdd.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(headerColumns)
    val data = rdd
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(row)

    //create Dataframe
    val dataFrame = spark.createDataFrame(data, schema)
    //returning header column and dataframe
    (headerColumns, dataFrame)
  }

  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  /** @return The schema of the DataFrame, assuming that the first given column has type String and all the others
    *         have type Double. None of the fields are nullable.
    * @param columnNames Column names of the DataFrame
    */
  def dfSchema(columnNames: List[String]): StructType = {
    val firstField = StructField(columnNames.head, StringType, nullable = false)
    val fields = columnNames.tail.map(field => StructField(field, DoubleType, nullable = false))
    StructType(firstField::fields)
  }


  /** @return An RDD Row compatible with the schema produced by `dfSchema`
    * @param line Raw fields
    */
  def row(line: List[String]): Row = {
    val list = line.head :: line.tail.map(_.toDouble)
    Row.fromSeq(list)
  }

  def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column]) = {
    def isPrimary(col: String): Boolean =
      col.startsWith("t01") ||
        col.startsWith("t03") ||
        col.startsWith("t11") ||
        col.startsWith("t1801") ||
        col.startsWith("t1803")

    def isWork(col: String): Boolean =
      col.startsWith("t05") ||
        col.startsWith("t1805")

    def isOther(col: String): Boolean =
      col.startsWith("t02") ||
        col.startsWith("t04") ||
        col.startsWith("t06") ||
        col.startsWith("t07") ||
        col.startsWith("t08") ||
        col.startsWith("t09") ||
        col.startsWith("t10") ||
        col.startsWith("t12") ||
        col.startsWith("t13") ||
        col.startsWith("t14") ||
        col.startsWith("t15") ||
        col.startsWith("t16") ||
        (col.startsWith("t18") && !(col.startsWith("t1801") || col.startsWith("t1803") || col.startsWith("t1805")))

    (columnNames.filter(isPrimary).map(column), columnNames.filter(isWork).map(column), columnNames.filter(isOther).map(column))
  }

  def timeUsageSummary(
                        primaryNeedsColumns: List[Column],
                        workColumns: List[Column],
                        otherColumns: List[Column],
                        df: DataFrame
                      ): DataFrame = {
    // Transform the data from the initial dataset into data that make
    // more sense for our use case
    // Hint: you can use the `when` and `otherwise` Spark functions
    // Hint: don’t forget to give your columns the expected name with the `as` method
    val workingStatusProjection: Column = when (df("telfs") < 3 , "working").otherwise("not working").as("working")
    val sexProjection: Column = when(df("tesex") === 1, "male").otherwise("female").as("sex")
    val ageProjection: Column = when(df("teage")>= 15 && df("teage")<= 22, "young")
      .when(df("teage")>=23 && df("teage")<=55, "active")
      .otherwise("elder")
      .as("age")

    // Create columns that sum columns of the initial dataset
    // Hint: you want to create a complex column expression that sums other columns
    //       by using the `+` operator between them
    // Hint: don’t forget to convert the value to hours
    val primaryNeedsProjection: Column =  primaryNeedsColumns
      .reduce(_ + _)
      .divide(60)
      .as("primaryNeeds")
    val workProjection: Column = workColumns
      .reduce(_ + _)
      .divide(60)
      .as("work")
    val otherProjection: Column =  otherColumns
      .reduce(_ + _)
      .divide(60)
      .as("other")
    df.select(workingStatusProjection, sexProjection, ageProjection, primaryNeedsProjection, workProjection, otherProjection).where("telfs <= 4")
    //      .where("telfs" <= 4) // Discard people who are not in labor force
  }
  def timeUsageGrouped(summed: DataFrame):DataFrame ={
    summed.select("working", "sex", "age", "primaryNeeds", "work", "other")
      .groupBy("working", "sex", "age")
      .agg(round(avg("primaryNeeds"), 1).as("primaryNeeds"),
        round(avg("work"), 1 ).as("work"),
        round(avg("other"), 1 ).as("other"))
      .sort("working","sex", "age")

  }


}