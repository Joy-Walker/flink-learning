package com.geekbang.flink.sideoutput;

import com.geekbang.flink.wordcount.util.WordCountData;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 *  侧输出，用于数据分流，主流和侧输出流之间互不影响
 */
public class SIdeOutputTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> dataStream = env.fromElements(WordCountData.WORDS);

        OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

        SingleOutputStreamOperator<String> processStream = dataStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                if (value.length() >= 100) {
                    ctx.output(outputTag, value);
                } else {
                    out.collect(value);
                }
            }
        });
        processStream.getSideOutput(outputTag).print();
        processStream.print();

        env.execute();


    }
}
