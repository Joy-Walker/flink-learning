package com.geekbang.flink.state.keyedstate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SumJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new CustomNoParallelSource()).map(new CustomMapFunction()).keyBy(0).map(new StateSumMap()).print();
        env.execute();

    }


    static class CustomMapFunction implements MapFunction<Long, Tuple2<String,Long>> {

        @Override
        public Tuple2<String,Long> map(Long value) throws Exception {
            String[] arr = {"a","b","c"};
            int index = value.intValue() % arr.length;
            return new Tuple2<>(arr[index], value);
        }
    }

    static class StateSumMap extends RichMapFunction<Tuple2<String,Long>, Tuple2<String,Long>> {

        // keyby后其实不用mapState统计，单纯为了测试
        MapState<String,Long> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String,Long> descriptor = new MapStateDescriptor<>(
                    "mapState",
                    String.class,
                    Long.class
            );
            mapState = getRuntimeContext().getMapState(descriptor);
            super.open(parameters);
        }

        @Override
        public Tuple2<String, Long> map(Tuple2<String, Long> stringLongTuple2) throws Exception {
            String key = stringLongTuple2.f0;
            Long value = stringLongTuple2.f1;
            if (mapState.contains(key)) {
                Long sum = mapState.get(key);
                sum += value;
                mapState.put(key, sum);
            } else {
                mapState.put(key, value);
            }
            return new Tuple2<>(key, mapState.get(key));
        }
    }
}
