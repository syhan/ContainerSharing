package com.sap.seawide

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object ContainerProducer {
  def main(args: Array[String]): Unit = {
    val producer = new KafkaProducer[String, String](config())

    CmaCgmContainerGenerator.generate().foreach(n => producer.send(new ProducerRecord[String, String]("vendors","cmacgm", n)))

    producer.close()
  }

  def config(): Properties = {
    val props = new Properties

    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", classOf[StringSerializer])
    props.put("value.serializer", classOf[StringSerializer])

    props
  }
}

class ContainerGenerator(prefix: Seq[String]) {
//  val boundary: Int = 10000000
  val boundary: Int = 200

  private def isValid(num: String): Boolean = { // https://en.wikipedia.org/wiki/ISO_6346
    val dict = "0123456789A?BCDEFGHIJK?LMNOPQRSTU?VWXYZ"

    val sum = num.zipWithIndex.slice(0, 10).foldLeft(0d)((acc, t) => acc + Math.pow(2, t._2) * dict.indexOf(t._1))

    dict.indexOf(num.charAt(10)) == ((sum % 11) % 10)
  }

  def generate(): Seq[String] = {
    Range(1, boundary).toStream.flatMap(n => prefix.map(p => "%s%07d".format(p, n))).filter(isValid)
  }

}

object MaerskContainerGenerator extends ContainerGenerator(Seq("MSKU"))
object CoscoContainerGenerator extends ContainerGenerator(Seq("CCLU", "CSLU"))
object CmaCgmContainerGenerator extends ContainerGenerator(Seq("CMAU", "CMCU"))
