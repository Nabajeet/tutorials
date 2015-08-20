package models.crawlers

/**
 * @author pallab
 */

import play.api.db.slick.Config.driver.simple._
import play.api.db.slick.DB
import play.api.Play.current
import play.api.Logger
import models.crawlers._
import utils.crawlers.Helpers._
import utils.crawlers._
import scala.slick.jdbc.{ GetResult, StaticQuery => Q }
import Q.interpolation
import controllers.crawlers.ProdMergeController
import com.github.nscala_time.time.Imports._
import com.github.tototoshi.slick.JodaSupport._
import play.api.cache.Cache
import javax.sql.rowset.serial.SerialBlob
import scala.concurrent.duration._
import java.sql._
import scala.util.Try

object DAO {

  /******************************************   MERCHANTS    ******************************************/
  object Merchants {

    case class Merchant(affiliateId: Int, slug: String, name: String, code: String,
                        trackingUrlPre: Option[String], trackingUrlPost: Option[String], affPram1: Option[String], affPram1Val: Option[String], affPram2: Option[String],
                        imageUrl: String, offerText: Option[String], crawlPerPage: Option[Int], crawlPageCounter1: Option[Int], crawlPageCounter2: Option[Int],
                        createdAt: Long, updatedAt: Long)

    def table = new DBTables.Merchants("pc_merchants")

    def findBySlug(slug: String): Option[Merchant] = Cache.getOrElse("merchant_" + slug, 3600)(table.findBySlug(slug))
  }
  /*******************************   MERCHANT PRODUCT FEED APIS  OR FILE DUMPS  ******************************************/
  object MerchantApi {

    case class MerchantApi(pathSlug: String, categoryGroupSlug: String, name: String, catId: Option[String], url: String,
                           isCrawling: Boolean, createdAt: LocalDateTime, updatedAt: LocalDateTime, crawledAt: LocalDateTime)

    def table(merchantSlug: String) = new DBTables.MerchantApis("pc_api_" + merchantSlug)

    def listAll(merchantSlug: String): List[MerchantApi] = table(merchantSlug).listAll

    def findByPathSlug(pathSlug: String, merchantSlug: String): Option[MerchantApi] = table(merchantSlug).findByPathSlug(pathSlug)

    def updateOrSave(merchantSlug: String, api: MerchantApi): Int = table(merchantSlug).updateOrSave(api)
  }
  /******************************************   MERCHANT CATEGORIES    ******************************************/
  object MerchantCategories {

    case class MerchantCategory(categoryPath: String, merchant: String, mappedId: Int, mappedSlug: String,
                                vPointByMerchant: Option[String], sampleProd: String, createdAt: LocalDateTime)

    val table = new DBTables.MerchantCategorys("pc_merchant_category")

    def save(sc: MerchantCategory) = table.save(sc)
    def update(sc: MerchantCategory) = table.update(sc)
    def search(merchants: List[String], onlyUnassigned: Boolean, offset: Int, limit: Int): List[MerchantCategory] = DB.withSession { implicit session: Session =>
      if (onlyUnassigned) {
        Query(table).where(x => (x.mappedId === Defaults.unassignedCategory && x.merchant.inSet(merchants))).drop(offset).take(limit).list
      } else Query(table).filter(x => x.merchant.inSet(merchants)).drop(offset).take(limit).list
    }
  }
  /******************************************   VANTAGECIRCLE MENU    ******************************************/
  object VcMenu {

    case class ProductCategory(id: Option[Int], charId: String, name: String, slug: String, isVisible: Boolean, isBrandedPage: Boolean,
                               url: String, vPoints: Option[String], imageUrl: String)

    val table = new DBTables.ProductCategories("pc_vc_menu")

    def getAll = table.getAll

    def saveNewMenu(c: Seq[DAO.VcMenu.ProductCategory]) = table.saveAll(c)

    def findById(id: Int): Option[ProductCategory] = Cache.getOrElse("VC_MENU_" + id, 1800)(table.findById(id))
  }
  /******************************************   BRANDS    ******************************************/
  object Brands {

    case class Brand(slug: String, name: String, priority: Int)
    val table = new DBTables.Brands("pc_brands")

    def saveOrUpdate(brand: DAO.Brands.Brand) = table.saveOrUpdate(brand)

  }
  /******************************************   Crawled Data    ******************************************/
  object CrawlDatas {

    case class CrawlData(id: Option[Int], category: String, data: Option[Blob], createdAt: LocalDateTime)

    def table(merchant: String, categoryGroup: String) = new DBTables.CrawlDatas("pc_crawl_data_" + merchant + "_" + categoryGroup)

    def insert(merchantSlug: String, categoryGroup: String, data: Option[Blob]) = table(merchantSlug, categoryGroup).save(CrawlData(None, categoryGroup, data, LocalDateTime.now))

  }

  /******************************************  Raw Products    ******************************************/

  object RawProducts {

    case class RawProduct(id: Option[Int], prodId: String, //2
                          name: Option[String], sellPrice: Option[Int], MRP: Option[Int], brand: Option[String], url: Option[String], //5
                          imageSmall: Option[String], imageLarge: Option[String], color: Option[String], Size: Option[String], //4
                          categoryPath: Option[String], desc: Option[String], spec: Option[String], inStock: Option[Boolean], //4
                          discount: Option[String], shippingCharge: Option[String], //2
                          COD: Option[Boolean], EMI: Option[Boolean], offers: Option[String]) //3

    def table(merchant: String, category: String) = new DBTables.RawProducts("pc_raw_data_" + merchant + "_" + category)

    def insert(merchant: String, categoryGroup: String, p: RawProduct) = {
      if (p.name.isDefined && p.sellPrice.isDefined && p.url.isDefined && p.categoryPath.isDefined) {
        table(merchant, categoryGroup).saveOrUpdate(p)
      } else {
        Logger.info("Failed pre-insert check :price :" + p.sellPrice + " name:" + p.name + " url:" + p.url)
      }
    }
  }
  /******************************************  PRICE HISTORY    ******************************************/

  object PriceHistory {

    case class PricePoint(idByMerchant: String, timestamp: LocalDateTime, sellPrice: Int, listPrice: Option[Int])

    def table = new DBTables.PriceHistory("pc_price_history")

    def insert(p: PricePoint) = DB.withSession { implicit session: Session =>
      table.insert(p)
    }
  }
  /******************************************   CATALOG    ******************************************/

  object ProductCatalog {
    case class Product(vcId: Option[Int], idByMerchant: String, univProdId: Option[String], //3
                       prodName: Option[String], brandSlug: Option[String], color: Option[String], size: Option[String], //4
                       imageSmall: Option[String], imageLarge: Option[String], //2
                       highlights: Option[String], basicFeatures: Option[String], desc: Option[String], spec: Option[String], //4
                       rank: Option[Int], isMergedToOther: Boolean, //2
                       createdAt: LocalDateTime, updatedAt: LocalDateTime) //2
    case class ProductDetailByMerchant(idByMerchant: String, vcId: Int, merchantSlug: String, sellPrice: Option[Int], listPrice: Option[Int],
                                       landingPage: Option[String], shippingCharge: Option[String], discount: Option[String],
                                       COD: Option[Boolean], EMI: Option[Boolean], offers: Option[String],
                                       inStock: Option[Boolean], categoryPath: Option[String], updatedAt: LocalDateTime)

    private def prodTable = new DBTables.Products("pc_products_new")

    private def productDetailByMerchantTable = new DBTables.ProductDetailByMerchants("pc_prod_detail_new")
    //re-implement using plain sql , since lenngth is inefficient
    def length = DB.withSession { implicit session: Session => prodTable.length.run }

    /************ SAVE OR UPDATE **************/
    def saveOrUpdate(prod: Product, prodDet: ProductDetailByMerchant): Int = {

      if (prod.prodName.isDefined && prodDet.sellPrice.isDefined && prodDet.sellPrice.get != 0 && prodDet.landingPage.isDefined && prodDet.categoryPath.isDefined) {

        val mcat = MerchantCategories.MerchantCategory(prodDet.categoryPath.get, prodDet.merchantSlug,
          Defaults.unassignedCategory, VcMenu.findById(Defaults.unassignedCategory).map(_.slug).getOrElse("unassigned"),
          null, prodDet.landingPage.get, LocalDateTime.now)
        MerchantCategories.save(mcat)
        val brand = prod.brandSlug.map { bName => Brands.Brand(Helpers.slugify(bName), bName, 1) }
        brand.map(Brands.saveOrUpdate(_))

        productDetailByMerchantTable.getByMerchantId(prod.idByMerchant).map { existingProdDet => //only need to update seller table
          val newProdDet = prodDet.copy(vcId = existingProdDet.vcId, idByMerchant = existingProdDet.idByMerchant,
            updatedAt = LocalDateTime.now)
          productDetailByMerchantTable.update(newProdDet)

          PriceHistory.insert(PriceHistory.PricePoint(existingProdDet.idByMerchant, LocalDateTime.now, prodDet.sellPrice.get, prodDet.listPrice))
          existingProdDet.vcId
        }.getOrElse { //insert both in prod and seller table
          // Try { 
          val newVcId = prodTable.save(prod.copy(brandSlug = brand.map(_.slug)))
          // }.toOption.map { newVcId =>
          val prodDetToSave = prodDet.copy(vcId = newVcId)
          //  Try { 
          productDetailByMerchantTable.save(prodDetToSave)
          //     }.getOrElse( Logger.info("Failed insert to ProdDet Table:" + prod.idByMerchant))
          PriceHistory.insert(PriceHistory.PricePoint(prodDet.idByMerchant, LocalDateTime.now, prodDet.sellPrice.get, prodDet.listPrice))
          newVcId
          //          }.getOrElse {
          //            Logger.info("Failed insert to Prod Table:" + prod.idByMerchant)
          //            0
          //          }
        }
      } else {
        Logger.info("Failed pre-insert check :price :" + prodDet.sellPrice + " name:" + prod.prodName + " url:" + prodDet.landingPage)
        0
      }
    }

    /*************** JOIN FOR INDEXING TO ES ***************/
    case class DetailedProduct(product: Product, merchants: List[(Merchants.Merchant, ProductDetailByMerchant, Option[String], List[PriceHistory.PricePoint])],
                               category: VcMenu.ProductCategory, brand: Brands.Brand)

    /*Execution exception[[SlickException: Read NULL value for column pc_prod_detail_new.id_by_merchant]]
     * inner join worked. may be due to not having prod in details table corresponding to entry in
     *  products table
     */
    def getDetailedProductInBatches(offset: Int, limit: Int) = DB.withSession { implicit session: Session =>
      (for {
        (prod, prodDet) <- prodTable.drop(offset).take(limit).filter(_.isMergedToOther === false) innerJoin productDetailByMerchantTable on (_.vcId === _.vcId)
        merchCat <- MerchantCategories.table filter { mCat => mCat.categoryPath === prodDet.categoryPath && mCat.merchant === prodDet.merchantSlug && mCat.mappedId =!= Defaults.unassignedCategory }
        category <- VcMenu.table.filter(_.id === merchCat.mappedId)
        brand <- Brands.table if brand.slug is prod.brandSlug
        merchant <- Merchants.table if merchant.slug is prodDet.merchantSlug
        priceHistory <- PriceHistory.table if priceHistory.idByMerchant is prodDet.idByMerchant
      } yield (prod, prodDet, category, brand, merchant, merchCat.vPointByMerchant, priceHistory)).list
        .groupBy(_._1).values.map { plist =>
          DetailedProduct(plist.head._1,
            plist.map { case (a, b, c, d, e, f, g) => (e, b, f, g) }.groupBy(_._2).values.map(l => (l.head._1, l.head._2, l.head._3, l.map(_._4))).toList,
            plist.head._3, plist.head._4)
        }.toSeq

    }

    def mergeProd(deactivateIds: List[Int], u: ProdMergeController.UpdatedProduct) = DB.withSession { implicit session: Session =>

      val res = session.withTransaction(
        deactivateIds.map { id => prodTable.where(_.vcId === id).map(_.isMergedToOther).update(true) },
        deactivateIds.map { id => productDetailByMerchantTable.where(_.vcId === id).map(_.vcId).update(u.vcId) },
        prodTable.where(_.vcId === u.vcId)
          .map { x => (x.prodName ~ x.imageSmall ~ x.imageLarge ~ x.desc ~ x.spec ~ x.color ~ x.size) }
          .update((Some(u.name.replace("(", "").replace(")", "")), u.imageSmall, u.imageLarge, u.desc, u.spec, u.color, u.size)))
      println("Done updating :" + res)
    }

    //OLD // in use by fk, az, sd
    //    def insertOrUpdate(input: DAO.RawProducts.RawProduct, merchant: Merchants.Merchant) = {
    //
    //      if (input.name.isDefined && input.sellPrice.isDefined && input.url.isDefined && input.categoryPath.isDefined) {
    //        productDetailByMerchantTable.getByMerchantId(merchant.code + input.prodId).map { prod => //only need to update seller table
    //
    //          val sd: ProductDetailByMerchant = ProductDetailByMerchant(prod.idByMerchant, prod.vcId, merchant.slug, input.sellPrice,
    //            input.MRP, input.url, input.shippingCharge, input.discount, None, None,
    //            modifyPriceHistory(prod.priceHistory, input.sellPrice, input.MRP),
    //            input.offers, input.inStock, input.categoryPath, LocalDateTime.now)
    //          productDetailByMerchantTable.update(sd)
    //          prod.vcId
    //        }.getOrElse { //insert both in prod and seller table
    //
    //          val pd: Product = Product(None, merchant.code + input.prodId, None, input.name, input.brand, None, None,
    //            input.imageSmall, input.imageLarge,
    //            None, input.desc, input.spec, Some(Defaults.defualtProdRank), false, LocalDateTime.now, LocalDateTime.now)
    //
    //          val VCID = prodTable.save(pd)
    //
    //          val sd: ProductDetailByMerchant = ProductDetailByMerchant(merchant.code + input.prodId, VCID, merchant.slug, input.sellPrice, input.MRP,
    //            input.url, input.shippingCharge, input.discount, None, None,
    //            modifyPriceHistory(None, input.sellPrice, input.MRP),
    //            input.offers, input.inStock, input.categoryPath,  LocalDateTime.now)
    //          productDetailByMerchantTable.save(sd)
    //          VCID
    //        }
    //      } else {
    //        Logger.info("Failed pre-insert check :price :" + input.sellPrice + " name:" + input.name + " url:" + input.url)
    //        0
    //      }
    //    }
  }

  object MergeSuggestions {
    val sugTable = DB.withSession { implicit session: Session => new CatalogDAO.MergeSuggestions("pc_merge_suggest") }
    def bulkInsert(sugs: List[(String, Option[String], Option[String])], seller: String) = {
      sugs.map { s =>
        sugTable.insert(
          CatalogDB.MergeSuggestion(None, s._1, s._2, s._3, Helpers.Time.asStamp, false))
      }
    }
    def getInBatches(offset: Int, limit: Int): List[CatalogDB.MergeSuggestion] = sugTable.getInBatches(offset: Int, limit: Int)
    def setAsDone(id: Int) = sugTable.setAsDone(id)
  }
  /******************************************   Category Keys    ******************************************/
  object CategoryKeys {
    case class CategoryKey(categoryName: String, keys: String)

    private def table = new DBTables.CategoryKeys("pc_category_keys")

    def insertOrUpdate(input: CategoryKey) {
      table.saveOrUpdate(input)
    }

    def getKeys(categoryName: String): Option[String] = {
      table.findByCategoryName(categoryName).map(_.keys)
    }
  }

  /******************************************   AMAZON    ******************************************/
  //  object Amazon {
  //    private val prodTable = new RawCatalogDAO.Products("pc_raw_amazon") //(DB.createSession())
  //    private val sellerCatTable = new RawCatalogDAO.SellerCategories("pc_seller_category2")
  //
  //    object Product {
  //      def insertOrUpdate(p: AmazonActor.AzProduct) = {
  //        val price1 = p.item_price.toIntOpt
  //        if (price1.isDefined && !p.item_name.isEmpty() && !p.item_page_url.isEmpty()) {
  //
  //          val rawprod = RawCatalogDB.RawProduct(None, p.item_unique_id, Some(p.parent_asin),
  //            p.item_name, price1.get, p.fm_price.map(_.toIntOpt).flatten, p.item_brand, p.item_page_url,
  //            p.item_image_small, p.item_image_med, p.item_image_large, None,
  //            p.merch_cat_path, p.item_short_desc, None, None,
  //            None, p.item_shipping_charge.map(_.toIntOpt).flatten,
  //            None, None, p.item_salesrank.map(_.toIntOpt).flatten)
  //
  //          sellerCatTable.saveOrUpdate(RawCatalogDB.SellerCategory(None, p.merch_cat_path, "amazon", Defaults.unassignedCategory, null, "", Helpers.Time.asStamp))
  //          prodTable.saveOrUpdate(rawprod)
  //        } else {
  //          Logger.info("Failed pre-insert check :price :" + p.item_price + " name:" + p.item_name + " url:" + p.item_page_url)
  //        }
  //      }
  //      def count: Long = prodTable.getCount
  //      def getInBatches(offset: Int, limit: Int) = prodTable.getInBatches(offset, limit)
  //
  //      def getTopRankingProds(upto: Int, cat: Int) = {
  //        (Q[(String, Option[String], Option[String])] +
  //          """select 
  //            p.`name`, p.`brand`, c.`slug`
  //            from pc_raw_amazon as p
  //            left join
  //            pc_seller_category2 as sc 
  //              on p.`category_path` = sc.`name`
  //            left join pc_categories as c
  //              on sc.`mapped_id` = c.`id`
  //            where p.`rank` < """ + upto.toString() + """ and sc.`mapped_id` = """ + cat.toString() +
  //          """ ORDER BY p.`rank` asc
  //            """).list
  //
  //      }
  //
  //    }
  //  }

  /******************************************   FLIPKART    ******************************************/
  //  object Flipkart {
  //    private val prodTable = new RawCatalogDAO.Products("pc_raw_flipkart")
  //    private val sellerCatTable = new RawCatalogDAO.SellerCategories("pc_seller_category2")
  //    private val seedTable = new RawCatalogDAO.Seeds("pc_seeds_flipkart")
  //    private val crawlTable = new RawCatalogDAO.CrawlDatas("pc_crawl_data_flipkart")
  //
  //    object Product {
  //      def insertOrUpdate(p: FlipkartActors.FkProduct) = {
  //        if (p.name.isDefined && p.sellPrice.isDefined && p.url.isDefined) {
  //          val colSku: Set[String] = if (p.colorSKU.isDefined) p.colorSKU.get.replace("[", "").replace("]", "").split(",").toSet else Set()
  //          val sizSku: Set[String] = if (p.sizeSKU.isDefined) p.sizeSKU.get.replace("[", "").replace("]", "").split(",").toSet else Set()
  //          val sku = (colSku ++ sizSku).mkString(",")
  //
  //          val rawprod = RawCatalogDB.RawProduct(None, p.sourceId, Some(p.sourceId),
  //            p.name.get, p.sellPrice.get, p.maxPrice, p.brand, p.url.get,
  //            p.imageUrl_275x275, p.imageUrl_400x400, p.imageUrl_1100x1100, p.imageUrl_unknown,
  //            p.categ, p.desc, None, p.inStock,
  //            p.discountPercentage, None,
  //            Some(p.offers), Some(sku), None)
  //          sellerCatTable.saveOrUpdate(RawCatalogDB.SellerCategory(None, p.categ, "flipkart", Defaults.unassignedCategory, null, "", Helpers.Time.asStamp))
  //          prodTable.saveOrUpdate(rawprod)
  //
  //        } else {
  //          Logger.info("Failed pre-insert check :price :" + p.sellPrice + " name:" + p.name + " url:" + p.url)
  //        }
  //      }
  //      def count: Long = prodTable.getCount
  //      def getInBatches(offset: Int, limit: Int) = prodTable.getInBatches(offset, limit)
  //    }
  //
  //    object Seeds {
  //      def insert(seeds: List[FlipkartActors.SeedUrl]) = seedTable.refresh(
  //        seeds.map(seed => RawCatalogDB.Seed(None, seed.category, seed.url, Helpers.Time.asStamp)))
  //      def findById(id: Int) = seedTable.findById(id)
  //    }
  //
  //    object CrawlData {
  //      import java.sql.Blob
  //
  //      def insertOrUpdate(id: Int, cat: String, data: Option[Blob]) = crawlTable.updateOrSave(RawCatalogDB.CrawlData(id, cat, data, Helpers.Time.asStamp))
  //      def total: Int = crawlTable.getCount
  //      def getCatIdList(category: String, offset: Int, limit: Int) = crawlTable.getCatIdList(category, offset, limit)
  //      def getIdList(offset: Int, limit: Int) = crawlTable.getIdList(offset, limit)
  //      def findById(id: Int) = crawlTable.findById(id)
  //    }
  //  }

  /******************************************   Company URLs    ******************************************/
  //  object Url {
  //    val sourceUrl = new RawCatalogDAO.RetailStores("pc_company_url")
  //    object CrawlUrl {
  //      def insertOrUpdate(id: Option[Int], company: String, tag: String, path: String, url: String, status: Boolean, createdAt: LocalDateTime, updatedAt: Option[LocalDateTime]) = sourceUrl.updateOrSave(RawCatalogDB.RetailStore(id, company, tag, path, url, status, createdAt, updatedAt))
  //      def total: Int = sourceUrl.getCount
  //      def getCompanyUrlList(company: String, offset: Int, limit: Int) = sourceUrl.findByCompany(company, offset, limit)
  //      def getCompanyCategoryUrlList(company: String, category: String, offset: Int, limit: Int) = sourceUrl.findByCompanyandCategory(company, category, offset, limit)
  //    }
  //  }

  /******************************************   PAYTM    ******************************************/
  //  object Paytm {
  //    val sellerCatTable = new RawCatalogDAO.SellerCategories("pc_seller_category2")
  //    object category {
  //      def insertOrUpdate(category: String) = {
  //        sellerCatTable.saveOrUpdate(RawCatalogDB.SellerCategory(None, category, "paytm", Defaults.unassignedCategory, null, "", Helpers.Time.asStamp))
  //      }
  //    }
  //
  //    val crawlTable = new RawCatalogDAO.ExtractCrawlDatas("pc_paytm_crawl_data")
  //    object CrawlData {
  //      import java.sql.Blob
  //      def clean = crawlTable.clean
  //      def insert(id: Option[Int], sourceUrl: String, pageCount: Int, cat: String, data: Option[Blob]) = crawlTable.save(RawCatalogDB.ExtractCrawlData(id, sourceUrl, pageCount, cat, data))
  //      def total: Int = crawlTable.getCount
  //      def getCatIdList(category: String, offset: Int, limit: Int) = crawlTable.getCatIdList(category, offset, limit)
  //      def getIdList(offset: Int, limit: Int) = crawlTable.getIdList(offset, limit)
  //      def findById(id: Int) = crawlTable.findById(id)
  //    }
  //
  //    val prodTable = new RawCatalogDAO.PaytmCrawlDataProducts("pc_paytm_raw")
  //    object Product {
  //      def clean = prodTable.clean
  //      def insert(id: Option[Int], category: String, p: PaytmActors.PaytmCategoryGridLayout) = {
  //        prodTable.save(RawCatalogDB.PaytmCrawlDataProduct(id, p.productId, p.name, p.shortDesc, p.url, p.seourl, p.urlType, p.promoText, p.imageUrl, p.verticalId, p.verticalLabel, p.offerPrice, p.actualPrice, p.merchantName, p.stock, p.brand, p.tag, category, p.shippable, p.createdAt, p.updatedAt, p.discount))
  //      }
  //      def count: Long = prodTable.getCount
  //      def getInBatches(offset: Int, limit: Int) = prodTable.getInBatches(offset, limit)
  //    }
  //  }

  /******************************************   SNAPDEAL    ******************************************/
  //  object Snapdeal {
  //    val sellerCatTable = new RawCatalogDAO.SellerCategories("pc_seller_category2")
  //    object category {
  //      def insertOrUpdate(category: String) = {
  //        sellerCatTable.saveOrUpdate(RawCatalogDB.SellerCategory(None, category, "snapdeal", Defaults.unassignedCategory, null, "", Helpers.Time.asStamp))
  //      }
  //    }
  //
  //    val crawlTable = new RawCatalogDAO.ExtractCrawlDatas("pc_snapdeal_crawl_data")
  //    object CrawlData {
  //      import java.sql.Blob
  //      def clean = crawlTable.clean
  //      def insert(id: Option[Int], sourceUrl: String, pageCount: Int, cat: String, data: Option[Blob]) = crawlTable.save(RawCatalogDB.ExtractCrawlData(id, sourceUrl, pageCount, cat, data))
  //      def total: Int = crawlTable.getCount
  //      def getCatIdList(category: String, offset: Int, limit: Int) = crawlTable.getCatIdList(category, offset, limit)
  //      def getIdList(offset: Int, limit: Int) = crawlTable.getIdList(offset, limit)
  //      def findById(id: Int) = crawlTable.findById(id)
  //    }
  //
  //    val prodTable = new RawCatalogDAO.SnapdealCrawlDataProducts("pc_snapdeal_raw")
  //    object Product {
  //      def clean = prodTable.clean
  //      def insert(id: Option[Int], brand: String, category: String, p: SnapdealActors.SnapdealProduct) = {
  //        prodTable.save(RawCatalogDB.SnapdealCrawlDataProduct(id, p.catalogId.getOrElse(0), p.name.getOrElse(""), brand, p.displayPrice, p.price, ("http://www.snapdeal.com/" + p.pageUrl.getOrElse("")), p.imagePath.getOrElse(""), category, p.soldOut, p.percentOff))
  //      }
  //      def count: Long = prodTable.getCount
  //      def getInBatches(offset: Int, limit: Int) = prodTable.getInBatches(offset, limit)
  //    }
  //  }

  /******************************************   EBAY    ******************************************/
  //  object Ebay {
  //    val sellerCatTable = new RawCatalogDAO.SellerCategories("pc_seller_category2")
  //
  //    object category {
  //      def insertOrUpdate(category: String) = {
  //        sellerCatTable.saveOrUpdate(RawCatalogDB.SellerCategory(None, category, "ebay", Defaults.unassignedCategory, null, "", Helpers.Time.asStamp))
  //      }
  //    }
  //
  //    val crawlTable = new RawCatalogDAO.ExtractCrawlDatas("pc_ebay_crawl_data")
  //    object CrawlData {
  //      import java.sql.Blob
  //      def clean = crawlTable.clean
  //      def insert(id: Option[Int], sourceUrl: String, pageCount: Int, cat: String, data: Option[Blob]) = crawlTable.save(RawCatalogDB.ExtractCrawlData(id, sourceUrl, pageCount, cat, data))
  //      def total: Int = crawlTable.getCount
  //      def getCatIdList(category: String, offset: Int, limit: Int) = crawlTable.getCatIdList(category, offset, limit)
  //      def getIdList(offset: Int, limit: Int) = crawlTable.getIdList(offset, limit)
  //      def findById(id: Int) = crawlTable.findById(id)
  //    }
  //
  //    val prodTable = new RawCatalogDAO.EbayCrawlDataProducts("pc_ebay_raw")
  //    object Product {
  //      def clean = prodTable.clean
  //      def insert(id: Option[Int], pId: Long, product: String, brand: String, mrp: Option[Int], price: Option[Int], url: String, image: String, model: String, seller: String, desc: String, category: String) = {
  //        prodTable.save(RawCatalogDB.EbayCrawlDataProduct(id, pId, product, brand, mrp, price, url, image, model, seller, desc, category))
  //      }
  //      def count: Long = prodTable.getCount
  //      def getInBatches(offset: Int, limit: Int) = prodTable.getInBatches(offset, limit)
  //    }
  //  }

  /******************************************   JABONG    ******************************************/
  //  object Jabong {
  //    val sellerCatTable = new RawCatalogDAO.SellerCategories("pc_seller_category2")
  //    object category {
  //      def insertOrUpdate(category: String) = {
  //        sellerCatTable.saveOrUpdate(RawCatalogDB.SellerCategory(None, category, "jabong", Defaults.unassignedCategory, null, "", Helpers.Time.asStamp))
  //      }
  //    }
  //
  //    val crawlTable = new RawCatalogDAO.ExtractCrawlDatas("pc_jabong_crawl_data")
  //    object CrawlData {
  //      import java.sql.Blob
  //      def clean = crawlTable.clean
  //      def insert(id: Option[Int], sourceUrl: String, pageCount: Int, cat: String, data: Option[Blob]) = crawlTable.save(RawCatalogDB.ExtractCrawlData(id, sourceUrl, pageCount, cat, data))
  //      def total: Int = crawlTable.getCount
  //      def getCatIdList(category: String, offset: Int, limit: Int) = crawlTable.getCatIdList(category, offset, limit)
  //      def getIdList(offset: Int, limit: Int) = crawlTable.getIdList(offset, limit)
  //      def findById(id: Int) = crawlTable.findById(id)
  //    }
  //
  //    val prodTable = new RawCatalogDAO.JabongCrawlDataProducts("pc_jabong_raw")
  //    object Product {
  //      def clean = prodTable.clean
  //      def insert(id: Option[Int], pId: String, sku: String, name: String, status: String, brand: String, gender: String, imageUrl: String, isBundleProduct: Option[Boolean], actualPrice: Int, discount: Int, offerPrice: Int, url: String, desc: String, color: String, category: String) = {
  //        prodTable.save(RawCatalogDB.JabongCrawlDataProduct(id, pId, sku, name, status, brand, gender, imageUrl, isBundleProduct, actualPrice, discount, offerPrice, url, desc, color, category))
  //      }
  //      def count: Long = prodTable.getCount
  //      def getInBatches(offset: Int, limit: Int) = prodTable.getInBatches(offset, limit)
  //    }
  //  }

  /******************************************   CATALOG    ******************************************/

  //  object Catalog {
  //    private val prodTable = new CatalogDAO.Products("pc_products")
  //    private val sellerDetailTable = new CatalogDAO.SellerDetails("pc_prod_sellers")
  //
  //    private val sellerCatTable = new RawCatalogDAO.SellerCategories("pc_seller_category2")
  //
  //    def length() = {
  //      prodTable.length.run
  //    }
  //    def insertOrUpdate(input: RawCatalogDB.RawProduct, seller: CatalogDB.Seller) = {
  //
  //      sellerDetailTable.getBySellerId(seller.code + input.prodId).map { prod => //only need to update seller table
  //        val sd: CatalogDB.SellerDetail = CatalogDB.SellerDetail(prod.sellerId, prod.seller, prod.vcId, input.price1,
  //          input.price2, input.url, input.shippingCharge, input.offers, input.inStock, input.categoryPath, Helpers.Time.asStamp)
  //        sellerDetailTable.update(sd)
  //      }.getOrElse { //insert both in prod and seller table
  //        val pd: CatalogDB.Product = CatalogDB.Product(None, input.name, input.brand,
  //          input.imageMedium, input.imageMedium, input.imageLarge, input.imageExtra,
  //          input.categoryPath, input.desc, input.spec, Defaults.defualtProdRank, Helpers.Time.asStamp)
  //
  //        val id = prodTable.save(pd)
  //
  //        val sd: CatalogDB.SellerDetail = CatalogDB.SellerDetail(seller.code + input.prodId, seller.slug, id, input.price1, input.price2,
  //          input.url, input.shippingCharge, input.offers, input.inStock, input.categoryPath, Helpers.Time.asStamp)
  //        sellerDetailTable.save(sd)
  //      }
  //    }
  //    /*
  //     * Used to update info about individual prod 
  //     */
  //    def updatePoduct(sid: String, p1: Option[Int], p2: Option[Int], url: Option[String], shipp: Option[Int], offers: Option[String], inStock: Option[Boolean], site: String) = {
  //
  //      sellerDetailTable.getBySellerId(sid).map { prod =>
  //        val sd: CatalogDB.SellerDetail = CatalogDB.SellerDetail(prod.sellerId, site, prod.vcId, p1.getOrElse(prod.price1),
  //
  //          p2, url.getOrElse(prod.url),
  //          if (shipp.isDefined) shipp else prod.shipping,
  //          if (offers.isDefined) offers else prod.offers,
  //          if (inStock.isDefined) inStock else prod.inStock, prod.sellerCatPath,
  //          Helpers.Time.asStamp)
  //        sellerDetailTable.update(sd)
  //      }
  //    }
  //    def mergeProd(req: ProdMergeController.MergeReq) = {
  //      Logger.info("Merge Req : " + req)
  //      val res = DB.withTransaction(
  //        prodTable.update(CatalogDB.Product(Some(req.toThis), req.asProd.name, req.asProd.brand,
  //          req.asProd.imageSmall, None, req.asProd.imageLarge, None,
  //          "", req.asProd.desc, req.asProd.spec, 1, Helpers.Time.asStamp)),
  //        req.mergeThese.map(sellerDetailTable.updateVcId(_, req.toThis)),
  //        req.mergeThese.map(prodTable.delete(_)))
  //      println("Done updating :" + res)
  //    }
  //    case class Seller(sellerId: String, seller: String, price1: Int, price2: Option[Int], url: String,
  //                      vp: Option[String], vpPercnt: Option[Int], shipngCharge: Option[Int], offers: List[String],
  //                      inStock: Option[Boolean], category: String, updatedAt: Long)
  //    case class EsProduct(vcId: Int, name: String, brand: Option[String], images: Map[String, Option[String]],
  //                         category: String, desc: Option[String], spec: Option[String], sellers: List[Seller])
  //
  //    def getDetailedProductInBatches(from: Int, to: Int): List[EsProduct] = {
  //      val esProds = (Q[(Int, String, Option[String], Option[String], Option[String], Option[String], Option[String], String, Option[String], Option[String], String, String, Int, Option[Int], String, String, Option[Int], Option[String], Option[Boolean], String, Long)] +
  //        """select 
  //            p.`vc_id`, p.`prod_name`, p.`brand`, p.`image_small`, p.`image_medium`, p.`image_large`, p.`image_extra`,
  //            p.`category_id`, p.`desc`, p.`spec`, s.`seller_id`, s.`seller`, s.`price1`, s.`price2`, s.`url`, s.`url`, s.`shipping`, s.`offers`, s.`in_stock`, s.`category`, s.`updated_at`
  //            from pc_products as p
  //            left join 
  //            pc_prod_sellers as s 
  //            on p.`vc_id` = s.`vc_id`
  //            where p.`vc_id` between """ +? from + " and " +? to +
  //        """ ORDER BY s.`price1` asc
  //           """).list
  //
  //      esProds.map { ep =>
  //        val menu = SellerCategories.getCategoryCharIdAndVp(ep._20, ep._12) //catPath , seller return catslug, vpseller, vpcat
  //        val vp: Option[String] = menu.map(m => m._2.getOrElse(m._3.getOrElse("")))
  //        ep.copy(_8 = menu.map(_._1).getOrElse(""), _16 = vp)
  //      }.groupBy { x => (x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10) }.map {
  //        case (k, v) =>
  //          EsProduct(k._1, k._2, k._3, Map("small" -> k._4, /* "medium" -> k._5,*/ "large" -> k._6 /*, "extra" -> k._7*/ ),
  //            k._8, k._9, k._10, v.map { y => Seller(y._11, y._12, y._13, y._14, y._15, y._16, None, y._17, y._18.map(_.split("/").toList).getOrElse(List()), y._19, y._20, y._21) })
  //      }
  //        .toList
  //    }
  //  }
  //  object SellerCategories {
  //    private val sellerCatTable = new RawCatalogDAO.SellerCategories("pc_seller_category2")
  //
  //    def search(q: Option[String], seller: Option[String], status: Boolean) = {
  //      val filters = (List(
  //        q.map("sc.`name` LIKE '%" + _ + "%' "),
  //        seller.map("sc.`seller` IN ('" + _.replaceAll(",", "\',\'") + "') "),
  //        if (!status) Some("sc.`mapped_id` =" + Defaults.unassignedCategory) else None).flatten).mkString(" and ")
  //      val query = """select * from pc_seller_category2 as sc where """ + filters
  //
  //      (Q[(Int, String, String, Int, String, String, Long)] + query).list
  //    }
  //    def getCategoryWithMenuMap =
  //      Cache.getOrElse[Map[String, (String, Option[String], Option[String])]]("sellerCatAndMenu", expiration = 60) {
  //        val m = (for {
  //          (sc, menu) <- sellerCatTable leftJoin CatalogDB.Categories on (_.mappedId === _.id)
  //        } yield (sc, menu) //(sc.name+"/"+sc.seller,(menu.charId,sc.sellerVPointPrcnt,menu.vPoints))
  //        ).list
  //        m.map { case (sc, menu) => (sc.name + "/" + sc.seller, (menu.charId, sc.sellerVPointPrcnt, menu.vPoints)) }.toMap
  //      }
  //    def getCategoryCharIdAndVp(cat: String, sellr: String): Option[(String, Option[String], Option[String])] =
  //      Cache.getOrElse[Option[(String, Option[String], Option[String])]]("catmenu" + cat + sellr, expiration = 60) {
  //        val l = getCategoryWithMenuMap.get(cat + "/" + sellr)
  //        Logger.info("cat menu for : " + cat + "/" + sellr + " : " + l)
  //        l
  //      }
  //
  //    def update(sellerList: List[RawCatalogDB.SellerCategory]) = sellerList.map(sellerCatTable.update(_))
  //  }

  //  object Categories {
  //
  //    def getAll = CatalogDB.Categories.getAll
  //    def getById(id: Int) = CatalogDB.Categories.findById(id)
  //
  //  }
  //
  //  object Sellers {
  //    private val sellersTable = new CatalogDAO.Sellers("pc_sellers")
  //    def findBySlug(slug: String): Option[CatalogDB.Seller] = (sellersTable.findById(slug))
  //  }
}