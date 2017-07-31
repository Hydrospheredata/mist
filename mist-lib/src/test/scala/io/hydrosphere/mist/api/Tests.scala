package io.hydrosphere.mist.api

import io.hydrosphere.mist.api.v2.{JobSuccess, JobInstances, Arg}

/**
  * Created by dos65 on 31.07.17.
  */
object Tests extends App {

  val map = Map(
   "n" -> 4,
   "name" -> "namename"
  )

 import JobInstances._

  val job = withArgs(
    Arg[Int]("n"),
    Arg[String]("name")
  ).withContext((n, name, context) => {
    JobSuccess(s"call $n + $name")
  })


  job.run(map, null)

}
