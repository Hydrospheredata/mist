package mist.api.jdsl

trait FuncOps {

  implicit class Func2Map[-T1, -T2, R](f: Func2[T1, T2, R]) {

    def map[G](g: R => G): (T1, T2) => G = (a,b) => g(f(a,b))
  }

  implicit class Func3Map[-T1, -T2, -T3, R](f: Func3[T1, T2, T3, R]) {

    def map[G](g: R => G): (T1, T2, T3) => G = (a,b,c) => g(f(a,b,c))
  }
}

object FuncOps extends FuncOps

