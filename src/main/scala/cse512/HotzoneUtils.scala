package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val rectangle_coordinates = queryRectangle.split(",").map(_.toFloat)
    val point_coordinates = pointString.split(",").map(_.toFloat)

    Check_Coordinates(rectangle_coordinates, point_coordinates)
  }

  def Check_Coordinates(rectangle_coordinates: Array[Float], point_coordinates: Array[Float]): Boolean ={
    if( rectangle_coordinates(0) <= point_coordinates(0) && point_coordinates(0) <= rectangle_coordinates(2)){
        if( rectangle_coordinates(1) <= point_coordinates(1) && point_coordinates(1) <= rectangle_coordinates(3)){
            return true
          }
      }
    false
  }

}