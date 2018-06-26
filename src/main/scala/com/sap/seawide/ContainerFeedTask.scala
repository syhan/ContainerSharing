package com.sap.seawide

import java.util

import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

class ContainerFeedTask extends SourceTask {
  override def start(props: util.Map[String, String]): Unit = ???

  override def poll(): util.List[SourceRecord] = ???

  override def stop(): Unit = ???

  override def version(): String = ???
}
