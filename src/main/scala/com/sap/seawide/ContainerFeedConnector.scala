package com.sap.seawide

import java.util

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

class ContainerFeedConnector extends SourceConnector {

  override def taskClass(): Class[_ <: Task] = classOf[ContainerFeedTask]

  override def start(props: util.Map[String, String]): Unit = ???

  override def stop(): Unit = ???

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = ???

  override def version(): String = ???

  override def config(): ConfigDef = ???
}
