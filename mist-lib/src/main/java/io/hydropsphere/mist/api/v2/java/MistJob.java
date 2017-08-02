package io.hydropsphere.mist.api.v2.java;

import io.hydrosphere.mist.api.v2.JobP;

public interface MistJob {

    <T> JobP<T> defineJob();
}
