package org.example;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import java.util.*;

public class RKGOperator {
    public static class Tuple {
        public String key;
        public String value;

        public Tuple() {}
        public Tuple(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class RKG extends RichFlatMapFunction<Tuple, Tuple> {
        private final int n;
        private final double theta;

        private MapState<String, Integer> keyToInstance;
        private ValueState<long[]> instanceLoads;

        public RKG(int n, double theta) {
            this.n = n;
            this.theta = theta;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Integer> keyDesc =
                    new MapStateDescriptor<>("keyToInstance", String.class, Integer.class);
            keyToInstance = getRuntimeContext().getMapState(keyDesc);

            ValueStateDescriptor<long[]> loadDesc =
                    new ValueStateDescriptor<>("instanceLoads", long[].class);
            instanceLoads = getRuntimeContext().getState(loadDesc);

            long[] loads = new long[n];
            instanceLoads.update(loads);
        }

        @Override
        public void flatMap(Tuple tuple, Collector<Tuple> out) throws Exception {
            long[] loads = instanceLoads.value();
            if (loads == null) {
                loads = new long[n];
            }

            Integer instance = keyToInstance.get(tuple.key);

            if (instance == null) {
                int hashedInstance = Math.abs(tuple.key.hashCode()) % n;
                int maxIdx = 0, minIdx = 0;
                long maxLoad = loads[0], minLoad = loads[0];
                for (int i = 1; i < n; i++) {
                    if (loads[i] > maxLoad) {
                        maxLoad = loads[i];
                        maxIdx = i;
                    }
                    if (loads[i] < minLoad) {
                        minLoad = loads[i];
                        minIdx = i;
                    }
                }

                double avg = Arrays.stream(loads).average().orElse(0.0);
                double imbalance = (maxLoad - avg) / (maxLoad == 0 ? 1 : maxLoad);

                if (hashedInstance == maxIdx && imbalance > theta) {
                    instance = minIdx;
                } else {
                    instance = hashedInstance;
                }

                keyToInstance.put(tuple.key, instance);
            }

            loads[instance] += 1;
            instanceLoads.update(loads);

            out.collect(new Tuple("instance-" + instance, tuple.value));
        }
    }
}
