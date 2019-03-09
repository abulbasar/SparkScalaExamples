package com.example.streaming.sinks

import com.datastax.driver.core.ProtocolOptions.Compression
import com.datastax.driver.core.{Cluster, HostDistance, PoolingOptions, Session}
import org.apache.spark.sql.{ForeachWriter, Row}

class CassandraSink(keySpace: String, contactPoints: String)
  extends ForeachWriter[Row] {

  private var session: Session = _

  def close(errorOrNull: Throwable): Unit = {
    if (!session.isClosed()) {
      session.close()
    }
  }

  def open(partitionId: Long, version: Long): Boolean = {

    val poolingOptions: PoolingOptions = new PoolingOptions()
      .setConnectionsPerHost(HostDistance.REMOTE, 1, 4)

    val cluster: Cluster = Cluster
      .builder()
      .addContactPoint(contactPoints)
      .withPoolingOptions(poolingOptions)
      .withCompression(Compression.SNAPPY)
      .build()

    session = cluster.connect(keySpace)
    true
  }

  def process(value: Row) {
    print(value)
  }
}
