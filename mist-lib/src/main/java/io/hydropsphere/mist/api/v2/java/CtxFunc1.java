package io.hydropsphere.mist.api.v2.java;

import org.apache.spark.api.java.JavaSparkContext;

public interface CtxFunc1<A, Out> {
    Out apply(A a, JavaSparkContext sc);
}

