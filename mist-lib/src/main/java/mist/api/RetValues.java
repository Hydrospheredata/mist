package mist.api;

import mist.api.jdsl.RetVal;
import mist.api.jdsl.RetVals;
import mist.api.jdsl.RetVals$;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class RetValues {

    private static RetVals instance = RetVals$.MODULE$;

    public static <T> RetVal<T> of(T val) {
        return instance.fromAny(val);
    }

    public static RetVal<Void> empty() {
        return instance.empty();
    }

    public static <T> RetVal<Optional<T>> some(T t) {
        return instance.fromAny(Optional.of(t));
    }

    public static <T> RetVal<Optional<T>> none() {
        return instance.fromAny(Optional.empty());
    }

    public static <T> RetVal<List<T>> of(List<T> it) {
        return instance.fromAny(it);
    }

    @SafeVarargs
    public static <T> RetVal<List<T>> of(T... ts) {
        return instance.fromAny(java.util.Arrays.asList(ts));
    }

    public static <T> RetVal<java.util.Map<String, T>> of(String k1, T v1) {
        HashMap<String, T> m = new HashMap<>(1);
        m.put(k1, v1);
        return of(m);
    }

    public static <T> RetVal<java.util.Map<String, T>> of(String k1, T v1, String k2, T v2) {
        HashMap<String, T> m = new HashMap<>(2);
        m.put(k1, v1);
        m.put(k2, v2);
        return of(m);
    }

    public static <T> RetVal<java.util.Map<String, T>> of(String k1, T v1, String k2, T v2, String k3, T v3) {
        HashMap<String, T> m = new HashMap<>(3);
        m.put(k1, v1);
        m.put(k2, v2);
        m.put(k3, v3);
        return of(m);
    }

    public static <T> RetVal<java.util.Map<String, T>> of(
            String k1, T v1,
            String k2, T v2,
            String k3, T v3,
            String k4, T v4
    ) {
        HashMap<String, T> m = new HashMap<>(4);
        m.put(k1, v1);
        m.put(k2, v2);
        m.put(k3, v3);
        m.put(k4, v4);
        return of(m);
    }

    public static <T> RetVal<java.util.Map<String, T>> of(
            String k1, T v1,
            String k2, T v2,
            String k3, T v3,
            String k4, T v4,
            String k5, T v5
    ) {
        HashMap<String, T> m = new HashMap<>(5);
        m.put(k1, v1);
        m.put(k2, v2);
        m.put(k3, v3);
        m.put(k4, v4);
        m.put(k5, v5);
        return of(m);
    }

}
