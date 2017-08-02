package io.hydropsphere.mist.api.v2.java;

import io.hydrosphere.mist.api.v2.*;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import scala.runtime.AbstractFunction2;

public class JJobArgs1<A> extends JobArgs1<A> {

    JJobArgs1(Arg<A> a) {
        super(a);
    }

    public <Out> Job1<A, Out> withContext2(CtxFunc1<A, Out> func) {
        return super.withContext(new AbstractFunction2<A, SparkContext, JobResult<Out>>() {
            @Override
            public JobResult<Out> apply(A a, SparkContext sc) {
                JavaSparkContext javaSc = new JavaSparkContext(sc);
                Out res = func.apply(a, javaSc);
                return new JobSuccess<>(res);
            }
        });
    }
}
