package models.crawlers

/**
 * @author pallab
 */

import play.api._
import play.api.db.slick.Config.driver.simple._
import play.api.db.slick.DB
import play.api.Play.current
import scala.slick.jdbc.meta._
import org.joda.time.LocalDateTime
import com.github.tototoshi.slick.JodaSupport._
import javax.sql.rowset.serial.SerialBlob
import java.sql._
import scala.slick.jdbc.{ GetResult, StaticQuery => Q }
import Q.interpolation
import play.api.cache.Cache

object DBTables {
  def isTableNotExists(tableName: String) = DB.withSession { implicit session: Session => MTable.getTables(tableName).list().isEmpty }

  /******************************************   MERCHANTS    ******************************************/
  class Merchants(tableName: String) extends Table[DAO.Merchants.Merchant](tableName) {
    def affiliateId = column[Int]("affiliate_id")
    def slug = column[String]("slug")
    def name = column[String]("name")
    def code = column[String]("code")
    def trackingUrlPre = column[Option[String]]("tracking_url_pre")
    def trackingUrlPost = column[Option[String]]("tracking_url_post")
    def affPram1 = column[Option[String]]("aff_param1")
    def affPram1Val = column[Option[String]]("aff_param1_value")
    def affPram2 = column[Option[String]]("aff_param2")
    def imageUrl = column[String]("image_url")
    def offerText = column[Option[String]]("offer_text")
    def crawlPerPage = column[Option[Int]]("crawl_per_page")
    def crawlPageCounter1 = column[Option[Int]]("crawl_page_counter1")
    def crawlPageCounter2 = column[Option[Int]]("crawl_page_counter2")
    def createdAt = column[Long]("created_at")
    def updatedAt = column[Long]("created_at")

    def * = affiliateId ~ slug ~ name ~ code ~ trackingUrlPre ~ trackingUrlPost ~ affPram1 ~ affPram1Val ~ affPram2 ~
      imageUrl ~ offerText ~ crawlPerPage ~ crawlPageCounter1 ~ crawlPageCounter2 ~ createdAt ~ updatedAt <> (DAO.Merchants.Merchant.apply _, DAO.Merchants.Merchant.unapply _)
    val bySlug = createFinderBy(_.slug)

    def findBySlug(slug: String): Option[DAO.Merchants.Merchant] = {
      DB.withSession { implicit session => this.bySlug(slug).firstOption }
    }
  }
  /*******************************   MERCHANT PRODUCT FEED APIS  OR FILE DUMPS  ******************************************/
  class MerchantApis(tableName: String) extends Table[DAO.MerchantApi.MerchantApi](tableName) {
    def pathSlug = column[String]("path_slug")
    def parentGroup = column[String]("parent_group")
    def name = column[String]("name")
    def catId = column[Option[String]]("cat_id")
    def url = column[String]("url", O.DBType("VARCHAR(2000)"))
    def isCrawling = column[Boolean]("is_crawling")
    def createdAt = column[LocalDateTime]("created_at")
    def updatedAt = column[LocalDateTime]("updated_at")
    def crawledAt = column[LocalDateTime]("crawled_at")

    def * = pathSlug ~ parentGroup ~ name ~ catId ~ url ~ isCrawling ~ createdAt ~ updatedAt ~ crawledAt <> (DAO.MerchantApi.MerchantApi.apply _, DAO.MerchantApi.MerchantApi.unapply _)
    val byPathSlug = createFinderBy(_.pathSlug)

    if (isTableNotExists(tableName)) {
      Logger.info("Creating Table :" + tableName)
      DB.withSession { implicit session: Session => this.ddl.create }
    }
    def listAll = DB.withSession { implicit session: Session => Query(this).list }

    def findByPathSlug(pathSlug: String): Option[DAO.MerchantApi.MerchantApi] = DB.withSession { implicit session: Session =>
      this.byPathSlug(pathSlug).firstOption
    }

    def updateOrSave(api: DAO.MerchantApi.MerchantApi): Int = DB.withSession { implicit session: Session =>
      this.byPathSlug(api.pathSlug)
        .firstOption.map { d =>
          this.where(_.pathSlug === d.pathSlug).update(api.copy(createdAt = d.createdAt, crawledAt = d.crawledAt))
        }.getOrElse {
          this.insert(api)
        }
    }
  }
  /******************************************   MERCHANT CATEGORIES    ******************************************/
  class MerchantCategorys(tableName: String) extends Table[DAO.MerchantCategories.MerchantCategory](tableName) {
    def categoryPath = column[String]("category_path")
    def merchant = column[String]("merchant")
    def mappedId = column[Int]("mapped_id")
    def mappedSlug = column[String]("mapped_slug")
    def vPointByMerchant = column[Option[String]]("vp_prcnt")
    def sampleProd = column[String]("sample_prod", O.DBType("VARCHAR(500)"))
    def createdAt = column[LocalDateTime]("created_at")

    def catMerchantIndx = index("IDX_MC_CM", (categoryPath, merchant), unique = true)

    def * = categoryPath ~ merchant ~ mappedId ~ mappedSlug ~ vPointByMerchant ~ sampleProd ~ createdAt <> (DAO.MerchantCategories.MerchantCategory.apply _, DAO.MerchantCategories.MerchantCategory.unapply _)

    val byCategoryPath = createFinderBy(_.categoryPath)
    if (isTableNotExists(tableName)) {
      Logger.info("Creating Table :" + tableName)
      DB.withSession { implicit session: Session => this.ddl.create }
    }

    def save(sc: DAO.MerchantCategories.MerchantCategory) = {

      val q = (Q.u + "INSERT INTO " + tableName + " VALUES ('" + sc.categoryPath + "','" + sc.merchant + "','" + sc.mappedId.toString() + "','" +
        sc.mappedSlug + "'," + null + ",'" + sc.sampleProd + "','" +
        sc.createdAt.toString() + "') ON DUPLICATE KEY UPDATE sample_prod='" + sc.sampleProd + "'")

      DB.withSession { implicit session: Session => q.execute }
    }

    def update(sellerCat: DAO.MerchantCategories.MerchantCategory) = DB.withSession { implicit session: Session =>
      this.where { x => (x.categoryPath === sellerCat.categoryPath && x.merchant === sellerCat.merchant) }.update(sellerCat)
    }
    def findByPath(cat: String, sellr: String): Option[Int] = DB.withSession { implicit session: Session =>
      Query(this).filter(x => x.categoryPath === cat && x.merchant === sellr).firstOption.map(_.mappedId)
    }
  }
  /******************************************   VANTAGECIRCLE MENU    ******************************************/
  class ProductCategories(tableName: String) extends Table[DAO.VcMenu.ProductCategory](tableName) {
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def charId = column[String]("char_id")
    def name = column[String]("name")
    def slug = column[String]("slug")
    def isVisible = column[Boolean]("is_visible")
    def isBrandedPage = column[Boolean]("is_branded")
    def url = column[String]("url")
    def vPoints = column[Option[String]]("cat_v_point")
    def imageUrl = column[String]("image_url")

    def * = id.? ~ charId ~ name ~ slug ~ isVisible ~ isBrandedPage ~ url ~ vPoints ~ imageUrl <> (DAO.VcMenu.ProductCategory.apply _, DAO.VcMenu.ProductCategory.unapply _)
    val byId = createFinderBy(_.id)
    def truncate: Int = DB.withSession { implicit session =>
      this.ddl.drop
      this.ddl.create
      1
    }
    def getAll: Seq[DAO.VcMenu.ProductCategory] = DB.withSession { implicit session =>
      val query = for (all <- this) yield all
      query.run
    }
    def saveAll(c: Seq[DAO.VcMenu.ProductCategory]): Option[Int] = DB.withSession { implicit session =>
      truncate
      this.insertAll(c: _*)
    }

    def findById(id: Int): Option[DAO.VcMenu.ProductCategory] = DB.withSession { implicit session: Session =>
      this.byId(id).firstOption
    }
  }
  /******************************************   BRANDS    ******************************************/

  class Brands(tableName: String) extends Table[DAO.Brands.Brand](tableName) {
    def slug = column[String]("slug", O.PrimaryKey)
    def name = column[String]("name")
    def priority = column[Int]("priority")

    def brandSlugIndex = index("IDX_BRAND_SLUG", slug, unique = true)
    def * = slug ~ name ~ priority <> (DAO.Brands.Brand.apply _, DAO.Brands.Brand.unapply _)

    if (isTableNotExists(tableName)) {
      Logger.info("Creating Table :" + tableName)
      DB.withSession { implicit session: Session => this.ddl.create }
    }
    def saveOrUpdate(b: DAO.Brands.Brand) = DB.withSession { implicit session: Session =>
      val q = (Q.u + "INSERT INTO " + tableName + " VALUES ('" + b.slug + "','" + b.name.replace("\'", "\'\'") + "','" + b.priority.toString() +
        "') ON DUPLICATE KEY UPDATE name='" + b.name.replace("\'", "\'\'") + "'")
      //println("Satement : "+ q.getStatement)
      q.execute
    }
  }
  /******************************************   Crawled Data    ******************************************/
  class CrawlDatas(tableName: String) extends Table[DAO.CrawlDatas.CrawlData](tableName) {

    def id = column[Int]("id", O.AutoInc, O.PrimaryKey)
    def category = column[String]("category")
    def data = column[Option[Blob]]("data", O.DBType("LONGBLOB"))
    def timeStamp = column[LocalDateTime]("created_at")
    def * = id.? ~ category ~ data ~ timeStamp <> (DAO.CrawlDatas.CrawlData.apply _, DAO.CrawlDatas.CrawlData.unapply _)

    def autoInc = * returning id

    if (isTableNotExists(tableName)) DB.withSession { implicit session: Session =>
      Logger.info("Creating Table :" + tableName)
      this.ddl.create
    }
    def save(data: DAO.CrawlDatas.CrawlData): Int = DB.withSession { implicit session: Session =>
      this.autoInc.insert(data)
    }

  }

  class RawProducts(tableName: String) extends Table[DAO.RawProducts.RawProduct](tableName) {
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def prodId = column[String]("prod_id", O.DBType("VARCHAR(100)"))

    def name = column[Option[String]]("name", O.DBType("VARCHAR(500)"))
    def price1 = column[Option[Int]]("sell_price")
    def price2 = column[Option[Int]]("mrp")
    def brand = column[Option[String]]("brand")
    def url = column[Option[String]]("url")

    def imageSmall = column[Option[String]]("image_small")
    def imageLarge = column[Option[String]]("image_large")
    def color = column[Option[String]]("color")
    def size = column[Option[String]]("size")
    def categoryPath = column[Option[String]]("category_path", O.DBType("VARCHAR(500)"))
    def desc = column[Option[String]]("desc", O.DBType("VARCHAR(25000)"))
    def spec = column[Option[String]]("spec", O.DBType("VARCHAR(25000)"))
    def inStock = column[Option[Boolean]]("in_stock")

    def discount = column[Option[String]]("discnt_prcnt")
    def shippingCharge = column[Option[String]]("shipng_chrge")

    def cod = column[Option[Boolean]]("cod")
    def emi = column[Option[Boolean]]("emi")
    def offers = column[Option[String]]("offers", O.DBType("VARCHAR(1000)"))

    def pIdIndex = index("IDX_PID_" + tableName, prodId, unique = true)
    def * = id.? ~ prodId ~
      name ~ price1 ~ price2 ~ brand ~ url ~
      imageSmall ~ imageLarge ~ color ~ size ~
      categoryPath ~ desc ~ spec ~ inStock ~
      discount ~ shippingCharge ~
      cod ~ emi ~ offers <> (DAO.RawProducts.RawProduct.apply _, DAO.RawProducts.RawProduct.unapply _)

    def autoInc = * //returning id

    val byProdId = createFinderBy(_.prodId)
    if (isTableNotExists(tableName)) DB.withSession { implicit session: Session =>
      Logger.info("Creating Table :" + tableName)
      this.ddl.create
    }

    def saveOrUpdate(input: DAO.RawProducts.RawProduct) = DB.withSession { implicit session: Session =>
      this.byProdId(input.prodId).firstOption.map { rawProd =>
        this.where(_.prodId === rawProd.prodId).update(input.copy(id = rawProd.id))
      }.getOrElse(this.autoInc.insert(input))
    }
  }
  /******************************************   PRICE HISTORY    ******************************************/

  class PriceHistory(tableName: String) extends Table[DAO.PriceHistory.PricePoint](tableName) {
    def idByMerchant = column[String]("id_by_merchant")
    def timestamp = column[LocalDateTime]("timestamp")
    def sellPrice = column[Int]("sell_price")
    def listPrice = column[Option[Int]]("mrp")

    def pHistoryIndex = index("IDX_PH", idByMerchant)
    def * = idByMerchant ~ timestamp ~ sellPrice ~ listPrice <> (DAO.PriceHistory.PricePoint.apply _, DAO.PriceHistory.PricePoint.unapply _)
    if (isTableNotExists(tableName)) DB.withSession { implicit session: Session =>
      Logger.info("Creating Table :" + tableName)
      this.ddl.create
    }
  }
  /******************************************   PRODUCTS    ******************************************/
  class Products(tableName: String) extends Table[DAO.ProductCatalog.Product](tableName) {
    def vcId = column[Int]("vc_id", O.PrimaryKey, O.AutoInc)
    def idByMerchant = column[String]("id_by_merchant")
    def univProdId = column[Option[String]]("unique_id")
    def prodName = column[Option[String]]("prod_name", O.DBType("VARCHAR(500)"))
    def brandSlug = column[Option[String]]("brand")
    def color = column[Option[String]]("color")
    def size = column[Option[String]]("size")
    def imageSmall = column[Option[String]]("image_small")
    def imageLarge = column[Option[String]]("image_large")

    def highlights = column[Option[String]]("highlights", O.DBType("VARCHAR(5000)"))
    def basicFeatures = column[Option[String]]("features", O.DBType("VARCHAR(5000)"))
    def desc = column[Option[String]]("desc", O.DBType("VARCHAR(20000)"))
    def spec = column[Option[String]]("spec", O.DBType("VARCHAR(20000)"))
    def rank = column[Option[Int]]("rank")
    def isMergedToOther = column[Boolean]("is_merged")
    def createdAt = column[LocalDateTime]("created_at")
    def updatedAt = column[LocalDateTime]("updated_at")

    def * = vcId.? ~ idByMerchant ~ univProdId ~ prodName ~ brandSlug ~ color ~ size ~ imageSmall ~ imageLarge ~
      highlights ~ basicFeatures ~ desc ~ spec ~ rank ~ isMergedToOther ~ createdAt ~ updatedAt <> (DAO.ProductCatalog.Product.apply _, DAO.ProductCatalog.Product.unapply _)

    def autoInc = * returning vcId
    val byVcId = createFinderBy(_.vcId)

    if (isTableNotExists(tableName)) DB.withSession { implicit session: Session =>
      Logger.info("Creating Table :" + tableName)
      this.ddl.create
    }

    def getByVCId(vcId: Int): Option[DAO.ProductCatalog.Product] = DB.withSession { implicit session: Session =>
      this.byVcId(vcId).firstOption
    }
    def save(input: DAO.ProductCatalog.Product): Int = DB.withSession { implicit session: Session =>
      this.autoInc.insert(input)
    }
    def update(p: DAO.ProductCatalog.Product) = DB.withSession { implicit session: Session =>
      this.where(_.vcId === p.vcId).update(p)
    }
    def updateRank(vcId: Int, rank: Int) = DB.withSession { implicit session: Session =>
      this.where(_.vcId === vcId).map(_.rank).update(Some(rank))
    }
    def delete(id: Int) = DB.withSession { implicit session: Session =>
      this.where(_.vcId === id).delete
    }
  }
  /******************************************   PRODUCT DETAILS    ******************************************/
  class ProductDetailByMerchants(tableName: String) extends Table[DAO.ProductCatalog.ProductDetailByMerchant](tableName) {
    def idByMerchant = column[String]("id_by_merchant", O.PrimaryKey)
    def vcId = column[Int]("vc_id")
    def merchantSlug = column[String]("seller")
    def sellPrice = column[Option[Int]]("sell_price")
    def mrp = column[Option[Int]]("mrp")
    def url = column[Option[String]]("landing_page")
    def shipping = column[Option[String]]("shipping")
    def discount = column[Option[String]]("discount")
    def cod = column[Option[Boolean]]("cod")
    def emi = column[Option[Boolean]]("emi")
    def offers = column[Option[String]]("offers", O.DBType("VARCHAR(2000)"))
    def inStock = column[Option[Boolean]]("in_stock")
    def categoryPath = column[Option[String]]("category_path")
    def updatedAt = column[LocalDateTime]("updated_at")

    def vcIdIndex = index("IDX_PD_VC_ID", vcId)
    def merchantIdIndex = index("IDX_PD_MID", idByMerchant, unique = true)

    def * = idByMerchant ~ vcId ~ merchantSlug ~ sellPrice ~ mrp ~ url ~ shipping ~ discount ~ cod ~ emi ~
      offers ~ inStock ~ categoryPath ~ updatedAt <> (DAO.ProductCatalog.ProductDetailByMerchant.apply _, DAO.ProductCatalog.ProductDetailByMerchant.unapply _)

    val byMerchantId = createFinderBy(_.idByMerchant)
    val byVcId = createFinderBy(_.vcId)

    if (isTableNotExists(tableName)) DB.withSession { implicit session: Session =>
      Logger.info("Creating Table :" + tableName)
      this.ddl.create
    }
    def getByMerchantId(sourceId: String): Option[DAO.ProductCatalog.ProductDetailByMerchant] = DB.withSession { implicit session: Session =>
      this.byMerchantId(sourceId).firstOption
    }
    def getByVcId(prodId: Int): List[DAO.ProductCatalog.ProductDetailByMerchant] = DB.withSession { implicit session: Session =>
      this.byVcId(prodId).list
    }
    def save(input: DAO.ProductCatalog.ProductDetailByMerchant) = DB.withSession { implicit session: Session =>
      this.insert(input)
    }
    def update(input: DAO.ProductCatalog.ProductDetailByMerchant) = DB.withSession { implicit session: Session =>
      this.where(_.idByMerchant === input.idByMerchant).update(input)
    }
    def updateVcId(fromThis: Int, toThis: Int) = DB.withSession { implicit session: Session =>
      this.where(_.vcId === fromThis).map(_.vcId).update(toThis)
    }
  }

  /******************************************   PRODUCT DETAILS    ******************************************/
  class CategoryKeys(tableName: String) extends Table[DAO.CategoryKeys.CategoryKey](tableName) {
    def categoryName = column[String]("category_name", O.PrimaryKey)
    def keys = column[String]("keys")

    def * = categoryName ~ keys <> (DAO.CategoryKeys.CategoryKey.apply _, DAO.CategoryKeys.CategoryKey.unapply _)

    val byCategoryName = createFinderBy(_.categoryName)

    def findByCategoryName(categoryName: String): Option[DAO.CategoryKeys.CategoryKey] = DB.withSession { implicit session =>
      this.byCategoryName(categoryName).firstOption
    }

    if (isTableNotExists(tableName)) {
      Logger.info("Creating Table :" + tableName)
      DB.withSession { implicit session: Session => this.ddl.create }
    }
    def saveOrUpdate(input: DAO.CategoryKeys.CategoryKey) = DB.withSession { implicit session: Session =>
      this.byCategoryName(input.categoryName).firstOption.map { rawCategory =>
        this.where(_.categoryName === rawCategory.categoryName).update(input)
      }.getOrElse(this.insert(input))
    }
  }

}