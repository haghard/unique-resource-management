package com.resource

object Implicits {

  implicit class ResourceOps(val self: com.resource.domain.Resource) extends AnyVal {
    def uniqueKey: String = self.name + ":" + self.version
  }
}

object tables {

  def resourceTableByUserId(tableNames: Vector[String], userId: String): String =
    tableNames(math.abs(userId.hashCode() % tableNames.size))
}
