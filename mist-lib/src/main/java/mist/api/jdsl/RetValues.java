package mist.api.jdsl;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class RetValues {

    public static RetVal of(Object val) {
        return RetVal.fromAny(val);
    }

    public static RetVal empty() {
        return RetVal.empty();
    }

    public static <T> RetVal some(T t) {
        return RetVal.fromAny(Optional.of(t));
    }

    public static RetVal none() {
        return RetVal.fromAny(Optional.empty());
    }

    public static <T> RetVal of(List<T> it) {
        return RetVal.fromAny(it);
    }

    @SafeVarargs
    public static <T> RetVal of(T... ts) {
        return RetVal.fromAny(java.util.Arrays.asList(ts));
    }

    public static <T> RetVal of(String k1, T v1) {
        HashMap<String, T> m = new HashMap<>(1);
        m.put(k1, v1);
        return of(m);
    }

    public static <T> RetVal of(String k1, T v1, String k2, T v2) {
        HashMap<String, T> m = new HashMap<>(2);
        m.put(k1, v1);
        m.put(k2, v2);
        return of(m);
    }

    public static <T> RetVal of(String k1, T v1, String k2, T v2, String k3, T v3) {
        HashMap<String, T> m = new HashMap<>(3);
        m.put(k1, v1);
        m.put(k2, v2);
        m.put(k3, v3);
        return of(m);
    }

    public static <T> RetVal of(
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

    public static <T> RetVal of(
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
