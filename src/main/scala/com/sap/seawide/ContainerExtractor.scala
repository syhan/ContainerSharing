
package com.sap.seawide

import java.util.function.Predicate

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark._
import play.api.libs.json.{JsUndefined, Json}
import scalaj.http.{Http, HttpResponse}

import scala.util.{Random, Try}


object ContainerExtractor {

//  val registry = Map[String, String => Try[HttpResponse[String]]](
//    "cmacgm" -> maersk
//  )

  val spark: SparkSession = SparkSession.builder
    .master("local[2]")
    .appName("Container Extractor")
    .config("es.index.auto.create", "true")
    .config("es.input.json", "true")
    .config("es.batch.size.entries", "8")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  val parameters: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "group.id" -> "42",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics: Iterable[String] = Array("vendors")

  def main(args: Array[String]): Unit = {
    val maersk = new Predicate[String, String] {
      override def test(t: String, c: String): Boolean = t.startsWith("maersk")
    }

    val rawStream: KStream[String, String] = new StreamsBuilder().stream("vendors")
    rawStream.branch(maersk)

    val ssc = new StreamingContext(sc, Seconds(5))
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, parameters)
    )

    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges


      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()
  }




  def cosco(sc: SparkContext): Unit = {
//    vendor(sc, Seq("CCLU", "CSLU"), "http://elines.coscoshipping.com/ebtracking/public/containers/")
////      .foreach(println)
//      .filter(_.isSuccess)
//      .map(_.get)
//      .map(r => Json.parse(r.body))
//      .filter(json => (json \ "code").as[String].equals("200") && (json \ "data" \ "content" \ "notFound").as[String].isEmpty)
//      .saveToEs("cosco/containers")
  }

  def query(containerNumber: String, endpoint: String, enableProxy: Boolean = true): Try[HttpResponse[String]] = {
    Thread.sleep(1000 + new Random().nextInt(2000)) // be polite and not too aggressive

    val url = endpoint + containerNumber

    println(s"Sending querying request to $url")

    Try(Http(s"$url")
      .proxy(randomProxy(containerNumber), 8080)
      .timeout(50000, 50000)
      .asString)
  }

  def maersk(numbers: RDD[String]): RDD[Try[HttpResponse[String]]] = {
    val results = numbers.map(query(_, "https://api.maerskline.com/track/")).persist()

//    results.filter()

    results
//    vendor(sc, Seq("MSKU"), "https://api.maerskline.com/track/")
//      .filter(_.isSuccess)
//      .map(_.get)
//      .filter(_.is2xx)
//      .map(_.body)
//      .saveToEs("maersk/containers")
    //      .foreach(println)
  }

  def cmacgm(sc: SparkContext): Unit = {
//    vendor(sc, Seq("CMAU", "CMCU"), "http://zhixiangsou.chinaports.com/CntrSearch/clientSearchByCntrNo?company=8&mode=cntr&cntrNo=")
//      .filter(_.isSuccess)
//      .map(_.get)
//      .map(r => Json.parse(r.body))
//      .filter(json => !(json \ "coscoCntrRecordList").isInstanceOf[JsUndefined])
////      .saveToEs("cmacgm/containers")
//      .foreach(println)
  }

  def randomProxy(num: String): String = {
    val domains: List[String] = List(/*"wdf", "pal", "sin", */"pvgl", "pek", "hkg"/*, "tyo", "nyc", "bos", "sfo", "phl" , "man", "muc", "fra", "ber", "vie"*/)
    val seed = num.substring(5).toInt * new Random().nextInt(1000)
    val domain = domains(seed % domains.size)

    s"proxy.$domain.sap.corp"
  }

}