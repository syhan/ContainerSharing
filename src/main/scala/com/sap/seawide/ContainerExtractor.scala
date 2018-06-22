
package com.sap.seawide

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._
import play.api.libs.json.{JsUndefined, Json}
import scalaj.http.{Http, HttpResponse}

import scala.util.{Random, Try}


object ContainerExtractor {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Container Extractor")
      .config("es.index.auto.create", "true")
      .config("es.input.json", "true")
      .config("es.batch.size.entries", "8")
      .getOrCreate()

    val sc = spark.sparkContext

//    cmacgm(sc)
    cosco(sc)
  }

  def cosco(sc: SparkContext): Unit = {
    vendor(sc, Seq("CCLU"), "http://elines.coscoshipping.com/ebtracking/public/containers/")
      .filter(_.isSuccess)
      .map(_.get)
      .map(r => Json.parse(r.body))
      .filter(json => (json \ "code").as[String].equals("200") && (json \ "data" \ "content" \ "notFound").as[String].isEmpty)
      .foreach(println)
//      .saveToEs("cosco/containers")
  }

  def maersk(sc: SparkContext): Unit = {
    vendor(sc, Seq("MSKU"), "https://api.maerskline.com/track/")
      .filter(_.isSuccess)
      .map(_.get)
      .filter(_.is2xx)
      .map(_.body)
      .saveToEs("maersk/containers")
    //      .foreach(println)
  }

  def cmacgm(sc: SparkContext): Unit = {
    vendor(sc, Seq("CMAU"), "http://zhixiangsou.chinaports.com/CntrSearch/clientSearchByCntrNo?company=8&mode=cntr&cntrNo=")
      .filter(_.isSuccess)
      .map(_.get)
      .map(r => Json.parse(r.body))
      .filter(json => !(json \ "coscoCntrRecordList").isInstanceOf[JsUndefined])
//      .saveToEs("cmacgm/containers")
      .foreach(println)
  }

  def valid(num: String): Boolean = { // https://en.wikipedia.org/wiki/ISO_6346
    val dict = "0123456789A?BCDEFGHIJK?LMNOPQRSTU?VWXYZ"

    val sum = num.zipWithIndex.slice(0, 10).foldLeft(0d)((acc, t) => acc + Math.pow(2, t._2) * dict.indexOf(t._1))

    dict.indexOf(num.charAt(10)) == ((sum % 11) % 10)
  }

  def serialNumber(sc: SparkContext, range: (Int, Int), prefix: Seq[String]): RDD[String] = {
    sc.parallelize(Range(range._1, range._2)).flatMap(s => prefix.map(p => "%s%07d".format(p, s))).filter(valid)
  }

  def vendor(sc: SparkContext, prefix: Seq[String], url: String): RDD[Try[HttpResponse[String]]] = {
    val q: String => Try[HttpResponse[String]] = (n: String) => {
      val proxy = randomProxy(n)

      Thread.sleep(1000) // be polite and not too aggressive

      println(s"Sending querying request to $url$n with $proxy...")

      Try(Http(s"$url$n")
        .proxy(proxy, 8080)
        .timeout(50000, 50000)
        .asString)//.recoverWith(q)
    }

    //    val range = 1000000
    val range = 5069050
    serialNumber(sc, (5069000, range), prefix).map(q)
  }

  def randomProxy(num: String): String = {
    val domains: List[String] = List(/*"wdf", "pal", "sin", */"pvgl", "pek", "hkg"/*, "tyo", "nyc", "bos", "sfo", "phl" , "man", "muc", "fra", "ber", "vie"*/)
    val seed = num.substring(5).toInt * new Random().nextInt(1000)
    val domain = domains(seed % domains.size)

    s"proxy.$domain.sap.corp"
  }

  def blacklist(number: String, domain: String): Unit = {

  }

}