package com.geekbang.flink.wordcount;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;


public class BroadcastTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        List<String> list = new ArrayList<>();
        list.add("zhangsan beijin");
        list.add("lisi shanghai");

        System.out.println("============broadcast===========");
        // broadcast 对流中的数据进行广播，会将数据广播给下游所有的子任务，有几个并行度就会广播几份，也就是紧接的下游算子的每个子任务都会收到一份数据
        // 如果没有broadcast，那么会将数据轮询分配给下游的子任务
        executionEnvironment.fromCollection(list).broadcast().map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] arr = s.split(" ");
                return String.join(":", arr);
            }
        }).setParallelism(4).print();


        executionEnvironment.execute();

    }
}
