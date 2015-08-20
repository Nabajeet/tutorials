package actors.crawlers

/**
 * @author pallab
 */

import models.crawlers._
import actors.crawlers._
import utils.crawlers._
import play.api._
import play.api.mvc._
import play.api.libs.concurrent.Akka
import play.api.Play.current
import akka.actor._
//import play.api.libs.concurrent.Execution.Implicits._
import actors.crawlers.Contexts.actorExecutionContext
import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.libs.ws._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.collection._
import java.sql.Blob
import javax.sql.rowset.serial.SerialBlob
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.LocalDateTime

object PayTmManager {

  def merchant = DAO.Merchants.findBySlug("paytm").get

  case class Category(name: String, id: Option[Int],
                      url: Option[String], subCategory: Option[List[Category]])
  case class Categories(cats: List[Category])
  //a treeNode is extension of MerchantApi by a HashMap of childrens
  case class TreeNode(pathSlug: String, categoryGroupSlug: String, name: String, catId: Option[String], url: String, isCrawling: Boolean,
                      createdAt: LocalDateTime, updatedAt: LocalDateTime, crawledAt: LocalDateTime,
                      subCategories: mutable.HashMap[String, TreeNode])
  implicit val catReads: Reads[Category] = (
    (__ \ "name").read[String] and
    (__ \ "category_id").readNullable[Int] and
    (__ \ "url").readNullable[String] and
    (__ \ "items").lazyReadNullable(Reads.list[Category](catReads)))(Category)
  implicit val pmCatReads: Reads[Categories] = (__ \ "items").read[List[Category]].map { Categories(_) }

  def getApis(refresh: Boolean): Future[JsObject] = {

    def makeMerchantApis(categories: List[Category]): Seq[DAO.MerchantApi.MerchantApi] = {
      import collection._
      val apis: mutable.Set[DAO.MerchantApi.MerchantApi] = mutable.Set()
      def loop(nodes: List[Category], categoryGroupSlug: String, pathSlug: String): Unit = {
        nodes.map { n =>
          val newPathSlug = if (pathSlug.isEmpty()) Helpers.slugify(n.name) else pathSlug + "/" + Helpers.slugify(n.name)
          apis.+=(DAO.MerchantApi.MerchantApi(
            newPathSlug, categoryGroupSlug, n.name, n.id.map(_.toString()), n.url.getOrElse(""), false,
            LocalDateTime.now(), LocalDateTime.now(), LocalDateTime.now()))
          loop(n.subCategory.getOrElse(List()), categoryGroupSlug, newPathSlug)
        }
      }
      categories.map { c => loop(List(c), Helpers.slugify(c.name), "") }
      apis.toSeq
    }

    def getMenuTree(apis: Seq[DAO.MerchantApi.MerchantApi]) = {
      val hashMap = new scala.collection.mutable.HashMap[String, TreeNode]
      apis.map(c =>
        hashMap.put(c.pathSlug, TreeNode(c.pathSlug, c.categoryGroupSlug, c.name, c.catId, c.url, c.isCrawling,
          c.createdAt, c.updatedAt, c.crawledAt, mutable.HashMap())))
      var rootNode: TreeNode = TreeNode("home", "", "Home", None, "", false, LocalDateTime.now(), LocalDateTime.now(), LocalDateTime.now(), mutable.HashMap())
      hashMap.keySet.map { k =>
        hashMap.get(k).map(insertInTree(_))
      }

      def insertInTree(d: TreeNode): TreeNode = {

        def loop(currDepth: Int, maxDepth: Int, currTree: TreeNode, n: TreeNode): TreeNode = {
          if (currDepth < maxDepth) {
            val nxtNode = currTree.subCategories.get(d.pathSlug.split("/")(currDepth)).getOrElse {
              val parentNodeNameSlug = d.pathSlug.split("/").init.mkString("/")
              insertInTree(hashMap.get(parentNodeNameSlug).get)
            }
            loop(currDepth + 1, maxDepth, nxtNode, d)
          } else if (currDepth == maxDepth) {
            currTree.subCategories.get(d.pathSlug.split("/")(currDepth)).map { existingNode =>
              existingNode
            }.getOrElse {
              currTree.subCategories.put(d.pathSlug.split("/")(currDepth), d)
              d
            }
          } else currTree
        }
        loop(0, d.pathSlug.split("/").length - 1, rootNode, d)
      }
      rootNode
    }
    def treeToJson(tree: TreeNode): JsObject = {
      def loop(api: TreeNode): JsObject =
        Json.obj(
          "categoryGroupSlug" -> api.categoryGroupSlug,
          "name" -> api.name,
          "pathSlug" -> api.pathSlug,
          "catId" -> api.catId,
          "url" -> api.url,
          "isCrawling" -> api.isCrawling,
          "crawlPerPage" -> merchant.crawlPerPage,
          "crawlPageCounter1" -> merchant.crawlPageCounter1,
          "crawlPageCounter2" -> merchant.crawlPageCounter2,
          //"createdAt" -> api.createdAt.toString("yyyy-MM-dd HH:mm:ss"),
          //"updatedAt" -> api.updatedAt.toString("yyyy-MM-dd HH:mm:ss"),
          //"crawledAt" -> api.crawledAt.toString("yyyy-MM-dd HH:mm:ss"),
          "subCategories" -> api.subCategories.map(x => loop(x._2)))
      Json.obj(
        "status" -> "Success",
        "merchant" -> merchant.slug,
        "categories" -> loop(tree).\("subCategories"))
    }
    val menuUrl = "https://catalog.paytm.com/v1//web/menu"
    if (refresh) {
      val result: Future[JsValue] = WS.url(menuUrl).get().map(_.json.as[JsValue])

      result.map { js =>
        js.validate[Categories].asOpt.map { categories =>
          val apis = makeMerchantApis(categories.cats)
          apis.map(DAO.MerchantApi.updateOrSave(merchant.slug, _))
          treeToJson(getMenuTree(apis))
        }.getOrElse {
          Json.obj("status" -> "Json Validation Error", "categories" -> JsArray())
        }
      }.fallbackTo {
        println(merchant.name + ": Api request FAILED")
        Future.successful(Json.obj("status" -> "Api Failure", "categories" -> JsArray()))
      }

    } else {
      Future.successful(treeToJson(getMenuTree(DAO.MerchantApi.listAll("paytm"))))
    }

  }

  /***************************Crawlers************************************************/

  val mainActor = Akka.system.actorOf(Props[MainActor], name = (merchant.slug + "MainActor"))
  //messages for MainActor
  case class CrawlCategory(apis: List[DAO.MerchantApi.MerchantApi], fromPage: Int)
  case class PriorityCrawlCategory(apis: List[DAO.MerchantApi.MerchantApi], fromPage: Int)
  case class CrawlProduct(prodUrls: List[String], catApi: Option[DAO.MerchantApi.MerchantApi])
  case class DoCrawl()
  case class CallComplete()
  case class ResetTimer()

  def crawl(apis: Seq[DAO.MerchantApi.MerchantApi]) = {
    println(merchant.name + ": sending apis to MainActor")
    mainActor ! CrawlCategory(apis.toList, 1) //ptm starts at 1 // perpage = 50
  }

  class MainActor extends Actor {

    //for debug purpose only
    Akka.system.scheduler.scheduleOnce(10000 milliseconds, self, ResetTimer)
    val maxAllowedConcurrentCall = 10
    var currentApiCallCounter = 0
    var completedApiCallCounter = 0

    //api ques priority wise :
    var productQueue = new mutable.Queue[(String, Option[DAO.MerchantApi.MerchantApi])]
    var priorityCategoryQueue = new mutable.Queue[(DAO.MerchantApi.MerchantApi, Int)]
    var stdCategoryQueue = new mutable.Queue[(DAO.MerchantApi.MerchantApi, Int)]

    def printStatus(calledBy: String) = {
      println(merchant.name + " STATUS : requested BY : " + calledBy +
        "\nLive                  :" + currentApiCallCounter +
        "\nCompleted             :" + completedApiCallCounter +
        "\nproductQueue          :" + productQueue.length +
        "\npriorityCategoryQueue :" + priorityCategoryQueue.length +
        "\nstdCategoryQueue      :" + stdCategoryQueue.length)
    }
    def receive = {

      case ResetTimer => {
        println("------------------" + merchant.name + "RESETING----------------------------------")
        currentApiCallCounter -= completedApiCallCounter
        completedApiCallCounter = 0
        self ! DoCrawl
        Akka.system.scheduler.scheduleOnce(1000 milliseconds, self, ResetTimer)
      }
      case CrawlProduct(urlList, api) => {
        println(merchant.name + ": crawl prod" + urlList.head)
        //urlList.map { url => productQueue += ((url, api)) }
        productQueue += ((urlList(2), api))
        productQueue += ((urlList(4), api))
        productQueue += ((urlList(6), api))
      }
      case PriorityCrawlCategory(apis, fromPage) => {
        apis.map { api => priorityCategoryQueue += ((api, fromPage)) }
      }
      case CrawlCategory(apis, fromPage) => {
        apis.map { api => stdCategoryQueue += ((api, fromPage)) }
      }
      case CallComplete => {
        completedApiCallCounter += 1
      }
      case DoCrawl => {
        /*
         * check if currentApiCallCounter is at maxAllowedConcurrentCall . if yes : ignore  else
         * check priorityQueue and crawl if there is any 
         * else check stdQueue and crawl if there is any
         */

        if (currentApiCallCounter < maxAllowedConcurrentCall) {

          if (!productQueue.isEmpty) {
            printStatus("productQueue")
            val worker = Akka.system.actorOf(Props[PayTmWorker], name = (merchant.slug + "Worker" + Helpers.getUUID()))
            val nextProd = productQueue.dequeue()
            worker ! ProcessProduct(nextProd._1, nextProd._2)
            currentApiCallCounter += 1
            self ! DoCrawl
          } else if (!priorityCategoryQueue.isEmpty) {
            printStatus("priorityCategoryQueue")
            val worker = Akka.system.actorOf(Props[PayTmWorker], name = (merchant.slug + "Worker" + Helpers.getUUID()))
            val nextApi = priorityCategoryQueue.dequeue()
            worker ! ProcessCategory(nextApi._1, nextApi._2)
            currentApiCallCounter += 1
            self ! DoCrawl
          } else if (!stdCategoryQueue.isEmpty) {
            printStatus("stdCategoryQueue")
            val worker = Akka.system.actorOf(Props[PayTmWorker], name = (merchant.slug + "Worker" + Helpers.getUUID()))
            val nextApi = stdCategoryQueue.dequeue()
            worker ! ProcessCategory(nextApi._1, nextApi._2)
            currentApiCallCounter += 1
            self ! DoCrawl
          } else {
            println("000000000000000000000" + merchant.name + " -Queues EMPTY-00000000000000000000000")
          }
        } else {
          println("xxxxxxxxxxxxxxxxxxxxxxxx" + merchant.name + " -AT MAX-xxxxxxxxxxxxxxxxxxxxxxx")
        }
      }
      case _ => {
        println(merchant.name + " MainActor has recieved unrecognized message")
      }
    }

  }

  case class ProcessCategory(api: DAO.MerchantApi.MerchantApi, fromPage: Int)
  case class ProcessProduct(prodUrl: String, catApi: Option[DAO.MerchantApi.MerchantApi])
  case class PaytmProduct(sourceId: Long, parentId: Option[Long], name: Option[String], brand: Option[String],
                          attributes: Option[JsValue], color: Option[String],
                          shortDesc: Option[String], spec: Option[JsValue], promoText: Option[String],
                          actualPrice: Option[Int], offerPrice: Option[Int],
                          largeImage: Option[String], smallImage: Option[String],
                          inStock: Option[Boolean], cod: Option[Int], landingPageUrl: Option[String],
                          discount: Option[String], categoryPath: String,
                          createdAt: LocalDateTime, updatedAt: LocalDateTime)

  implicit val readsJodaLocalDateTime = Reads[LocalDateTime](js =>
    js.validate[String].map[LocalDateTime](dtString =>
      LocalDateTime.parse(dtString, ISODateTimeFormat.dateTime())))

  def int2bool(n: Int): Boolean = if (n == 0) false else true
  implicit val productReads: Reads[PaytmProduct] = (
    (__ \ "product_id").read[Long] and
    (__ \ "parent_id").readNullable[Long] and
    (__ \ "name").readNullable[String] and
    (__ \ "brand").readNullable[String] and
    (__ \ "attributes").readNullable[JsValue] and
    (__ \ "attributes" \ "color").readNullable[String] and

    (__ \ "short_desc").readNullable[String].map { x => if (x.isEmpty) null else x } and
    (__ \ "long_rich_desc").readNullable[JsValue] and
    (__ \ "promo_text").readNullable[String] and

    (__ \ "actual_price").readNullable[Int] and
    (__ \ "offer_price").readNullable[Int] and

    (__ \ "image_url").readNullable[String] and
    (__ \ "thumbnail").readNullable[String] and

    (__ \ "instock").readNullable[Boolean] and

    (__ \ "pay_type_supported" \ "COD").readNullable[Int] and

    (__ \ "shareurl").readNullable[String] and
    (__ \ "discount").readNullable[String] and

    (__ \ "ancestors").read[Seq[JsValue]].map { l => l.init.last.\("url_key").as[String] } and

    Reads.pure(LocalDateTime.now()) and
    Reads.pure(LocalDateTime.now()))(PaytmProduct)

  class PayTmWorker extends Actor {
    def receive = {
      case ProcessCategory(api, fromPage) => {
        println(merchant.name + " Worker : Processing category " + api.pathSlug)
        val complxHolder = WS.url(api.url + "?page_count=" + fromPage).withRequestTimeout(60000)
        val result: Future[JsValue] = complxHolder.get().map(_.json.as[JsValue])

        result.onComplete { x =>
          val jsresult = x.get
          val hasNextUrl = jsresult.\("has_more").as[Boolean]
          val prodUrls = (jsresult.\("grid_layout") \\ ("url")).map(_.as[String])
          mainActor ! CrawlProduct(prodUrls.toList, Some(api))
          mainActor ! CallComplete
          if (hasNextUrl) {
            //mainActor ! CrawlCategory(List(api), fromPage + 1)
          }
        }
        self ! PoisonPill
      }
      case ProcessProduct(url, api) => {
        val complxHolder = WS.url(url).withRequestTimeout(60000)
        val result: Future[JsValue] = complxHolder.get().map(_.json.as[JsValue])
        val nameCleanUp = "[()]".r
        val attributCleanUp = "[{}]".r
        result.onComplete { x =>
          DAO.CrawlDatas.insert("paytm", api.map(_.categoryGroupSlug).getOrElse(""), Some(new SerialBlob(x.get.toString().getBytes)))

          //format product features

          x.get.validate[PaytmProduct] match {
            case jsProd: JsSuccess[PaytmProduct] => {
              val p: PaytmProduct = jsProd.get

              //              api.map { a => 
              //                val rawprod = DAO.RawProducts.RawProduct(None, merchant.code+p.sourceId,
              //                p.name, p.actualPrice, p.offerPrice, p.brand, p.landingPageUrl,
              //                p.smallImage, p.largeImage, p.color, None,
              //                Some(p.categoryPath), p.shortDesc, p.spec.map(_.toString()), p.inStock,
              //                p.discount, None,
              //                p.cod.map { n => int2bool(n) }, None, p.promoText)
              //                DAO.RawProducts.insert(merchant.slug, a.categoryGroupSlug, rawprod) }

              val highlights = getProductHighlights(p.attributes)
              val keyString = checkAndUpdateCategoryKeys(p.categoryPath, highlights)
              val catKeys = DAO.CategoryKeys.CategoryKey(p.categoryPath, keyString)

              println(catKeys)

              val prod = DAO.ProductCatalog.Product(None, merchant.code + p.sourceId, None,
                p.name.map { nameCleanUp.replaceAllIn(_, "") }, p.brand, p.color, None,
                p.smallImage, p.largeImage,
                Option(Json.stringify(highlights)), p.attributes.map(_.toString()), p.shortDesc, p.spec.map(_.toString()),
                None, false,
                LocalDateTime.now, LocalDateTime.now)
              val prodDet = DAO.ProductCatalog.ProductDetailByMerchant(merchant.code + p.sourceId, 0, merchant.slug, p.offerPrice, p.actualPrice,
                p.landingPageUrl, None, p.discount.map(d => if (d == "0%") null else d),
                p.cod.map { n => int2bool(n) }, None, p.promoText,
                p.inStock, Some(p.categoryPath), LocalDateTime.now())
              import scala.util.Try
              Try {
                DAO.ProductCatalog.saveOrUpdate(prod, prodDet)
                DAO.CategoryKeys.insertOrUpdate(catKeys)
              }.recover {
                case e: Exception => println(merchant.name + ": INSERT TO CATALOG FAILED WITH EXCEPTION : \n" + e.getMessage)
              }

            }
            case e: JsError => println(merchant.name + " PRODUCT VALIDATION ERROR \n" + e)
          }

          mainActor ! CallComplete
        }
        self ! PoisonPill
      }
      case _ => {
        println("PayTm Worker has recieved unrecognized message")
        mainActor ! CallComplete
        self ! PoisonPill
      }
    }
  }
  /*
   * This method removes the null values from the feature list
   */
  def getProductHighlights(jsonFeatures: Option[JsValue]): JsObject = {

    jsonFeatures match {
      case Some(jsonFeatures) => {
        var jsonObj: JsObject = jsonFeatures.as[JsObject]
        val keys = jsonObj.keys.toList
        for (key <- keys) {
          //remove null value fields
          if ((jsonObj \ key).equals(JsNull) || (jsonObj \ key).as[String].isEmpty() || key.endsWith("_filter"))
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