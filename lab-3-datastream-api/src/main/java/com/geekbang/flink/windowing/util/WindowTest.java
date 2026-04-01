package com.geekbang.flink.windowing.util;

import com.geekbang.flink.source.custom.CustomNoParallelSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class WindowTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
      env.addSource(new CustomNoParallelSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Long>() {
                    @Override
                    public long extractAscendingTimestamp(Long element) {
                        // 使用当前系统时间作为事件时间
                        return System.currentTimeMillis();
                    }
                }).keyBy(x -> 1)
                        .window(TumblingEventTimeWindows.of(Time.of(2, TimeUnit.SECONDS)))
                                .aggregate(new CountAgg(), new CountWindowFunction());

        env.execute();
    }

     static class CountAgg implements AggregateFunction<Long, Long, Long> {
        @Override
        public Long createAccumulator() {
            return null;
        }

        @Override
        public Long add(Long aLong, Long aLong2) {
            return null;
        }

        @Override
        public Long getResult(Long aLong) {
            return null;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    static class CountWindowFunction extends ProcessWindowFunction<Long, Long, Integer, TimeWindow> {


        @Override
        public void process(Integer integer, ProcessWindowFunction<Long, Long, Integer, TimeWindow>.Context context, Iterable<Long> elements, Collector<Long> out) throws Exception {

        }
    }
}
