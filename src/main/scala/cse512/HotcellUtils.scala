package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  var coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int = {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(", "").toDouble / coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")", "").toDouble / coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser(timestampString: String): Timestamp = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def calculateNeighbours(X: Float, Y: Float, Z: Float, minX: Float, minY: Float, minZ: Float, maxX: Float, maxY: Float, maxZ: Float): Int = {
    var count = 0
    var neighbours = Array(17, 11, 7)

    if (X == minX || X == maxX) {
      count = count + 1
    }
    if (Y == minY || Y == maxY) {
      count = count + 1
    }
    if (Z == minZ || Z == maxZ) {
      count = count + 1
    }

    if (count > 0) {
      return neighbours(count)
    }
    else{
      return 26
    }

  }

}
