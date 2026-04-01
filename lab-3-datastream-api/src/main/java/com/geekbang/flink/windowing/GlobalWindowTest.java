package com.geekbang.flink.windowing;

import com.geekbang.flink.source.custom.CustomNoParallelSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class GlobalWindowTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Long> windowStream = env.addSource(new CustomNoParallelSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Long>() {
                    @Override
                    public long extractAscendingTimestamp(Long element) {
                        // 使用当前系统时间作为事件时间
                        return System.currentTimeMillis();
                    }
                })
                .windowAll(GlobalWindows.create())
                .evictor(TimeEvictor.of(Time.of(2, TimeUnit.SECONDS)))
                .trigger(CountTrigger.of(10))
                .process(new ProcessAllWindowFunction<Long, Long, GlobalWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Long, Long, GlobalWindow>.Context context, Iterable<Long> elements, Collector<Long> out) throws Exception {
                        for (Long element : elements) {
                            out.collect(element);
                        }
                        System.out.println("\n");
                    }
                });

        windowStream.print();
        env.execute();
    }
}
