package cse512

import com.vividsolutions.jts.awt.PointShapeFactory.X
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>(
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      ))
    spark.udf.register("CalculateY",(pickupPoint: String)=>(
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      ))
    spark.udf.register("CalculateZ",(pickupTime: String)=>(
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      ))

    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.createOrReplaceTempView("pickupInfoView") // added
    pickupInfo.show()


    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    pickupInfo = spark.sql("select x,y,z,count(*) as hotcells from pickupInfoView group by x, y, z order by z,y,x")
    pickupInfo.createOrReplaceTempView("relevantCells")
    pickupInfo.show()

    println("Calculating Mean......")
    val hotcellSum = spark.sql("select sum(hotcells) from relevantCells").first().getLong(0).toDouble
    spark.udf.register("square", (X: Int) => (HotcellUtils.square(X)))
    val squareOfHotcells = spark.sql("select sum(square(hotcells)) as sumOfSquare from relevantCells").first().getDouble(0)

    val hotcellMean =  hotcellSum/numCells
    println(hotcellMean)

    println("Calculating STD.......")
    var hotcellStd = scala.math.sqrt((squareOfHotcells/numCells) - (hotcellMean*hotcellMean))
    println(hotcellStd)


    spark.udf.register("calculateNeighbours", (X: Float, Y: Float, Z:Float, minX: Float, minY: Float, minZ: Float, maxX: Float, maxY: Float, maxZ: Float)=>((
      HotcellUtils.calculateNeighbours(X, Y, Z, minX, minY, minZ, maxX, maxY, maxZ)
    )))

    println("Calculating Neighbours..........")
    val Neighbours = spark.sql("select calculateNeighbours(a1.x,a1.y,a1.z,"+minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + ") as ncount,count(*) as countall, a1.x as x,a1.y as y,a1.z as z, sum(a2.hotcells) as sumtotal from relevantCells as a1, relevantCells as a2 where (a2.x = a1.x+1 or a2.x = a1.x or a2.x = a1.x-1) and (a2.y = a1.y+1 or a2.y = a1.y or a2.y =a1.y-1) and (a2.z = a1.z+1 or a2.z = a1.z or a2.z =a1.z-1) group by a1.z,a1.y,a1.x order by a1.z,a1.y,a1.x")
    Neighbours.createOrReplaceTempView("Neighbours")
    Neighbours.show()


    spark.udf.register("zscore", (hotcellMean: Double, hotcellStd: Double, sumn: Double, countn: Double, numCells: Double)=> (
      HotcellUtils.calculateZscore(hotcellMean, hotcellStd, sumn, countn, numCells)
    ))


    var finalDf = spark.sql("select x,y,z, zscore("+ hotcellMean+","+hotcellStd+ ",sumtotal,ncount,"+ numCells +") as zs from Neighbours order by zs desc limit 50")
    finalDf.createOrReplaceTempView("FinalDf")
    finalDf.show()

    return spark.sql("select x,y,z from FinalDf")
  }
}