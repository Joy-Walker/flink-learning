package com.geekbang.flink.watermark;

import com.geekbang.flink.source.custom.CustomParallelSource;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class StreamingWindowWatermark3 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStream<Tuple2<String, Long>> waterMarkStream = env.addSource(new CustomParallelSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                // 第一个参数是当前元素，第二个参数是当前元素上一次分配的时间戳，如果没有，则是Long.MIN_VALUE
                                .withTimestampAssigner((element, recordTimestamp) -> {
//                                    System.out.println("element:" + element + " recordTimestamp:" + recordTimestamp);
                                    return System.currentTimeMillis();
                                })
                );

        waterMarkStream = waterMarkStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                // 第一个参数是当前元素，第二个参数是上一个的时间戳
                                .withTimestampAssigner((element, recordTimestamp) -> {
//                                    System.out.println("element:" + element + " recordTimestamp:" + recordTimestamp);
                                    return System.currentTimeMillis();
                                })
                );

        DataStream<String> windowStream = waterMarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
                .evictor(new Evictor<Tuple2<String, Long>, TimeWindow>() {
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Long>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
                        for (TimestampedValue<Tuple2<String, Long>> element : elements) {
                            System.out.println("evictBefore element:" + element);
                        }

                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Long>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
                        for (TimestampedValue<Tuple2<String, Long>> element : elements) {
                            System.out.println("evictAfter element:" + element);
                        }
                    }
                })
                .trigger(new Trigger<Tuple2<String, Long>, TimeWindow>() {

                    // 每来一个元素，都会调用

                    /**
                     *
                     * @param element The element that arrived.
                     * @param timestamp The timestamp of the element that arrived.
                     * @param window The window to which the element is being added.
                     * @param ctx A context object that can be used to register timer callbacks.
                     * @return
                     * @throws Exception
                     */
                    @Override
                    public TriggerResult onElement(Tuple2<String, Long> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        System.out.println("onElement 调用" + "element: " + element + " timestamp:" + timestamp + " window:" + window);
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        System.out.println("onProcessingTime 调用" + "time:" + time + " window:" + window);
                        return TriggerResult.CONTINUE;

                    }

                    //  窗口闭合的时候触发一次

                    /**
                     *
                     * @param time The timestamp at which the timer fired.
                     * @param window The window for which the timer fired.
                     * @param ctx A context object that can be used to register timer callbacks.
                     * @return
                     * @throws Exception
                     */
                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        System.out.println("onEventTime 调用" + "element: "  + " window:" + window);
                        return TriggerResult.FIRE_AND_PURGE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
                        System.out.println("clear 调用" + " window:" + window);
                    }
                })
                .apply(new AllWindowFunction<Tuple2<String, Long>, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple2<String, Long>> values, Collector<String> out) throws Exception {
//                        System.out.println("window:" + window);
                        for (Tuple2<String, Long> value : values) {
                            out.collect(window.getStart() + ":" +value.f0 + ":" + value.f1 + ":" + window.getEnd());
                        }
                        System.out.println("\n");

                    }
                });

//        windowStream.print();
        env.execute();


    }
}
