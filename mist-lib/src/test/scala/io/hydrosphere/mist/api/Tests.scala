package io.hydrosphere.mist.api

import io.hydrosphere.mist.api.v2.{JobInstances, Arg}
import io.hydrosphere.mist.apiv2.JobSuccess

/**
  * Created by dos65 on 31.07.17.
  */
object Tests extends App {

  val map = Map(
   "n" -> List(4, 3, 4),
   "name" -> "namename"
  )

 import JobInstances._

  val job = withArgs(
    Arg[Int]("n").seq,
    Arg[String]("name")
  ).withContext((n, name, context) => {
    JobSuccess(s"call $n + $name")
  })


  val x = job.run(map, null)
  println(x)

}
