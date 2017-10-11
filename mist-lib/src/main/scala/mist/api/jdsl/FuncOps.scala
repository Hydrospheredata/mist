package mist.api.jdsl

trait FuncOps {

  implicit class Func2Map[-T1, -T2, R](f: Func2[T1, T2, R]) {

    def map[G](g: R => G): (T1, T2) => G = (a,b) => g(f(a,b))

    def toScalaFunc: Function2[T1, T2, R] = (a1: T1, a2: T2) => f.apply(a1, a2)
  }

  implicit class Func3Map[-T1, -T2, -T3, R](f: Func3[T1, T2, T3, R]) {

    def map[G](g: R => G): (T1, T2, T3) => G = (a,b,c) => g(f(a,b,c))
    def toScalaFunc: Function3[T1, T2, T3, R] = (a1: T1, a2: T2, a3: T3) => f.apply(a1, a2, a3)
  }

}

object FuncOps extends FuncOps

