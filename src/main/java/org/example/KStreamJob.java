package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.KStreamOperator.Tuple;

public class KStreamJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("Live Streaming Comment")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        DataStream<Tuple> inputTuples = kafkaStream.map(line -> {
            String[] parts = line.split(",");
            String key = parts.length > 0 ? parts[0] : "";
            String value = parts.length > 1 ? parts[1] : "";
            return new Tuple(key, value);
        });
        int downstreamParallelism = 3;
        double theta = 0.2;
        int bfSize = 1024;
        int numHash = 2;
        DataStream<Tuple> kstreamOut = inputTuples.flatMap(
                new KStreamOperator.KStream(downstreamParallelism, theta, bfSize, numHash)
        );
        kstreamOut
                .keyBy(t -> t.key)
                .process(new DistinctValueCountProcessFunction())
                .print();
        env.execute("KStream Test Example");
    }
    public static class DistinctValueCountProcessFunction
            extends KeyedProcessFunction<String, KStreamOperator.Tuple, Tuple2<String, Integer>> {
        private transient MapState<String, Boolean> seenValues;
        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            MapStateDescriptor<String, Boolean> desc =
                    new MapStateDescriptor<>("seenValues", String.class, Boolean.class);
            seenValues = getRuntimeContext().getMapState(desc);
        }
        @Override
        public void processElement(
                KStreamOperator.Tuple value,
                Context ctx,
                Collector<Tuple2<String, Integer>> out) throws Exception {
            if (value.value != null && !value.value.isEmpty() && !seenValues.contains(value.value)) {
                seenValues.put(value.value, true);
            }
            int distinctCount = 0;
            for (Boolean ignored : seenValues.values()) {
                distinctCount++;
            }
            out.collect(Tuple2.of(value.key, distinctCount));
        }
    }
}
