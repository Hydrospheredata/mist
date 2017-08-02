package io.hydropsphere.mist.api.v2.java;

import io.hydrosphere.mist.api.v2.*;

public class JDsl {

    public static Arg<Integer> intArg(String name) {
        return Arg$.MODULE$.jIntArg(name);
    }

    public static Arg<String> stringArg(String name) {
        return Arg$.MODULE$.strArg(name);
    }

    public static <A> JJobArgs1<A> withArgs(Arg<A> arg) {
        return new JJobArgs1<>(arg);
    }
}

