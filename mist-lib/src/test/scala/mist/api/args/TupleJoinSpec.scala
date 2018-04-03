package mist.api.args

import mist.api.{FnContext, Handle}
import org.apache.spark.SparkContext
import org.scalatest.FunSpec

class TupleJoinSpec extends FunSpec {

  it("asdasd") {
    val x = TupleJoin((1, "sad"), 3)
    val x2 = TupleJoin(1, "x")
    println(x)
    println(x2)
    val x3 = TupleJoin(("z", 12), ("d", false, "asd"))
    println(x3)
  }

  it("args") {
    val arg1 = ArgDef.const("1")
    val arg2 = ArgDef.const(2)
    val arg3 = ArgDef.const(false)

    val res = Combiner(arg1, Combiner(arg2, arg3))
  }

  it("joiner") {

    val arg1 = ArgDef.const("1")
    val arg2 = ArgDef.const(2)
    val arg3 = ArgDef.const(false)

    val tuple = (arg1, arg2, arg3)
    val res = ArgDefJoiner(tuple)
  }

  it("alt build") {

    val arg1 = ArgDef.const("1")
    val arg2 = ArgDef.const(2)
    val arg3 = ArgDef.const(false)
    import WithArsScala2._
    import mist.api.Contexts._

    val z = withArs2(arg1).apply2((a: String) => 42)
    val xx = (a: String, b: Int, c: Boolean) => 42
    val xxxx = withArs2((arg1, arg2)).apply2((a: String, b: Int) => 42)

    println(xxxx.invoke(FnContext(Map.empty)))
  }

}
