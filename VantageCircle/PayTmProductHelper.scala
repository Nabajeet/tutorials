package utils.crawlers

import play.api.libs.json._
import scala.collection._
import models.crawlers._
import actors.crawlers._

/**
 * @author nabajeet
 *
 */
object PayTmProductHelper {
  /*
   * This method removes the null values from the feature list
   */
  def filterProductFeatures(features: JsValue): JsObject = {

    val jsonFeatures = (features \ "attributes").asOpt[JsValue]

    jsonFeatures match {
      case Some(jsonFeatures) => {
        var jsonObj: JsObject = jsonFeatures.as[JsObject]
        val keys = jsonObj.keys.toList
        for (key <- keys) {
          //remove null value fields
          if ((jsonObj \ key).equals(JsNull) || key.endsWith("_filter"))
            jsonObj = jsonObj - key
        }
        jsonObj
      }
      case None => {
        println(" FEATURE VALIDATION ERROR \n")
        null
      }
    }
  }

  def checkAndUpdateCategoryKeys(categoryPath: String, jsonObject: JsObject): String = {

    val newKeysSet: Set[String] = jsonObject.keys.toSet
    
    //Get old keys for category 
    val oldKeysStr: Option[String] = DAO.CategoryKeys.getKeys(categoryPath)

    oldKeysStr match {
      case Some(oldKeysStr) => {
        val oldKeysSet: Set[String] = oldKeysStr.split(",").toSet
        println(oldKeysSet.toString())

        val resultSet = oldKeysSet ++ newKeysSet

        resultSet.mkString(",")
      }
      case None => {
        newKeysSet.mkString(",")
      }
    }
  }
}
  //  case class GenericFeatures(
//    color: Option[String],//1
//    dialShape: Option[String],//2
//    fit: Option[String],//3
//    gender: Option[String],//4
//    size: Option[String],//5
//    strapColor: Option[String],//6
//    footwearType: Option[String],//7
//    occasion: Option[String],//8
//    sleeve: Option[String],//9
//    material:Option[String],//10
//    neckType:Option[String],//11
//    resolution:Option[String],//12
//    batteryType:Option[String],//13
//    lcdScreenSize:Option[String],//14
//    sensorType:Option[String],//15
//    opticalZoom:Option[String]//16
//  )
//
//  implicit val featureReads: Reads[GenericFeatures] = (
//    (__ \ "attributes" \ "color").readNullable[String] and//1
//    (__ \ "attributes" \ "dial_shape").readNullable[String] and//2
//    (__ \ "attributes" \ "fit").readNullable[String] and//3
//    (__ \ "attributes" \ "gender").readNullable[String] and//4
//    (__ \ "attributes" \ "size").readNullable[String] and//5
//    (__ \ "attributes" \ "strap_color").readNullable[String] and//6
//    (__ \ "attributes" \ "type").readNullable[String]and//7
//    (__ \ "attributes" \ "occasion").readNullable[String]and//8
//    (__ \ "attributes" \ "sleeve").readNullable[String]and//9
//    (__ \ "attributes" \ "material").readNullable[String]and//10
//    (__ \ "attributes" \ "neck_type").readNullable[String]and //11
//    (__ \ "attributes" \ "resolution").readNullable[String]and //12
//    (__ \ "attributes" \ "battery_type").readNullable[String]and//13
//    (__ \ "attributes" \ "lcd-screen_size").readNullable[String]and //14
//    (__ \ "attributes" \ "sensor_type").readNullable[String]and //15
//    (__ \ "attributes" \ "optical_zoom").readNullable[String] //16
//  )(GenericFeatures)
//
//  def generalizeProductFeatures(features: JsValue): Option[String] = {
//    features.validate[GenericFeatures] match {
//      case jsFeatures: JsSuccess[GenericFeatures] => {
//        val resultFeatures = jsFeatures.get
//        val productFeatures: JsObject = Json.obj(
//          "color" -> resultFeatures.color,//1
//          "dial_shape" -> resultFeatures.dialShape,//2
//          "fit" -> resultFeatures.fit,//3
//          "gender" -> resultFeatures.gender,//4
//          "size" -> resultFeatures.size,//5
//          "strap_color" -> resultFeatures.strapColor,//6
//          "type" -> resultFeatures.footwearType,//7
//          "occasion" -> resultFeatures.occasion,//8
//          "sleeve" -> resultFeatures.sleeve,//9
//          "material" -> resultFeatures.material,//10
//          "neck_type" -> resultFeatures.neckType,//11
//          "resolution" -> resultFeatures.resolution,//12
//          "battery_type"->resultFeatures.batteryType,//13
//          "lcd_screen_size"->resultFeatures.lcdScreenSize,//14
//          "sensor_type"->resultFeatures.sensorType,//15
//          "optical_zoom"->resultFeatures.opticalZoom   //16       
//          )
//        Option(Json.stringify(productFeatures))
//      }
//      case e: JsError => {
//        println(" FEATURE VALIDATION ERROR \n" + e)
//        null
//      }
//    }


//  val genericFeatures: JsValue = Json.obj(
  //    "color" -> JsNull,
  //    "dial_shape" -> JsNull,
  //    "fit" -> JsNull,
  //    "gender" -> JsNull,
  //    "material" -> JsNull,
  //    "size" -> JsNull,
  //    "strap_color" -> JsNull,
  //    "type" -> JsNull)
  //
  //    val watchFeatures = (
  //      (__ \ 'dial_shape).json.put(features \ "attributes" \ "dial_shape") and
  //        (__ \ 'gender).json.put(features \ "attributes" \ "gender") and
  //        (__ \ 'material).json.put(features \ "attributes" \ "material") and
  //        (__ \ 'strap_color).json.put(features \ "attributes" \ "strap_color") and
  //        (__ \ 'color).json.put(JsNull) and
  //        (__ \ 'fit).json.put(JsNull) and
  //        (__ \ 'size).json.put(JsNull) and
  //        (__ \ 'type).json.put(JsNull) reduce)
  //
  //    val apparelFeatures = (
  //      (__ \ 'color).json.put(features \ "attributes" \ "color") and
  //        (__ \ 'fit).json.put(features \ "attributes" \ "fit") and
  //        (__ \ 'gender).json.put(features \ "attributes" \ "gender") and
  //        (__ \ 'material).json.put(features \ "attributes" \ "material") and
  //        (__ \ 'size).json.put(features \ "attributes" \ "size") and
  //        (__ \ 'dial_shape).json.put(JsNull) and
  //        (__ \ 'strap_color).json.put(JsNull) and
  //        (__ \ 'type).json.put(JsNull) reduce)
  //
  //    val footwearFeatures = (
  //      (__ \ 'color).json.put(features \ "attributes" \ "color") and
  //        (__ \ 'gender).json.put(features \ "attributes" \ "gender") and
  //        (__ \ 'material).json.put(features \ "attributes" \ "material") and
  //        (__ \ 'size).json.put(features \ "attributes" \ "size") and
  //        (__ \ 'type).json.put(features \ "attributes" \ "type") and
  //        (__ \ 'fit).json.put(JsNull) and
  //        (__ \ 'dial_shape).json.put(JsNull) and
  //        (__ \ 'strap_color).json.put(JsNull) reduce)
  //        
  //    val noneFeatures = (
  //      (__ \ 'color).json.put(JsNull) and
  //        (__ \ 'dial_shape).json.put(JsNull) and
  //        (__ \ 'fit).json.put(JsNull) and
  //        (__ \ 'gender).json.put(JsNull) and
  //        (__ \ 'material).json.put(JsNull) and
  //        (__ \ 'size).json.put(JsNull) and
  //        (__ \ 'strap_color).json.put(JsNull) and
  //        (__ \ 'type).json.put(JsNull) reduce)
  //
  //    val productVerticalLabel = features.\("vertical_label").as[String]
  //
  //    val jsonResult = productVerticalLabel match {
  //      case "Watches"  => genericFeatures.transform(watchFeatures)
  //      case "Apparel"  => genericFeatures.transform(apparelFeatures)
  //      case "Footwear" => genericFeatures.transform(footwearFeatures)
  //      case _          => genericFeatures.transform(noneFeatures)
  //    }
  //
  //    jsonResult match {
  //      case s: JsSuccess[JsObject] => Option(Json.stringify(s.get))
  //      case e: JsError             => Option("something worng")
  //    }