/**
  * Created by bluejoe on 2020/3/8.
  */
trait TestCondition {
  //val zookeeperString = "localhost:2181"
  val zookeeperString = "10.0.85.107:2181"

  val SERVER_NODE_ID: Array[(Int, Int)];

  val GLOBAL_SETTING: Map[String, String];
}

class StrongMultiNode extends TestCondition {
  override val SERVER_NODE_ID = Array(1 -> 1224, 2 -> 1225, 3 -> 1226)
  override val GLOBAL_SETTING = Map[String, String](
    "zookeeper.address" -> zookeeperString,
    "replica.num" -> "3",
    "region.size.limit" -> "9000000",
    "blob.crc.enabled" -> "true",
    "region.version.check.interval" -> "1000",
    "consistency.strategy" -> "strong"
  )
}

class EventualMultiNode extends TestCondition {
  override val SERVER_NODE_ID = Array(1 -> 1224, 2 -> 1225, 3 -> 1226)
  override val GLOBAL_SETTING = Map[String, String](
    "zookeeper.address" -> zookeeperString,
    "replica.num" -> "3",
    "region.size.limit" -> "9000000",
    "blob.crc.enabled" -> "true",
    "region.version.check.interval" -> "1000",
    "consistency.strategy" -> "eventual"
  )
}

class SingleNode extends TestCondition {
  val SERVER_NODE_ID = Array(1 -> 1224)
  val GLOBAL_SETTING = Map[String, String](
    "zookeeper.address" -> zookeeperString,
    "replica.num" -> "1",
    "region.size.limit" -> "9000000",
    "blob.crc.enabled" -> "true",
    "region.version.check.interval" -> "1000"
  )
}