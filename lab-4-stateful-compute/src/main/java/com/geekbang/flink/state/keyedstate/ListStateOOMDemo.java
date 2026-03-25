package com.geekbang.flink.state.keyedstate;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * ListState OOM 风险演示
 * 
 * ❌ 错误示例：无限制地向 ListState 添加数据
 */
public class ListStateOOMDemo {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 模拟持续不断的数据流
        DataStreamSource<Tuple2<String, String>> source = env.fromElements(
            Tuple2.of("user1", "event1"),
            Tuple2.of("user1", "event2"),
            Tuple2.of("user1", "event3"),
            Tuple2.of("user1", "event4"),
            Tuple2.of("user1", "event5")
            // 想象这里有成千上万条数据...
        );
        
        source
            .keyBy(0)
            .map(new BadListStateFunction())
            .print();
        
        env.execute("ListState OOM Demo");
    }
    
    /**
     * ❌ 危险的用法 - 无限制累积数据
     */
    static class BadListStateFunction extends RichMapFunction<Tuple2<String, String>, String> {
        
        private transient ListState<String> eventHistoryState;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<String> descriptor = new ListStateDescriptor<>(
                "event_history",
                Types.STRING
            );
            eventHistoryState = getRuntimeContext().getListState(descriptor);
        }
        
        @Override
        public String map(Tuple2<String, String> value) throws Exception {
            // ❌ 危险：不断地添加数据，永不清理
            eventHistoryState.add(value.f1);
            
            // 获取所有历史数据（会越来越慢）
            StringBuilder sb = new StringBuilder();
            int count = 0;
            for (String event : eventHistoryState.get()) {
                sb.append(event).append(",");
                count++;
            }
            
            return "Key: " + value.f0 + ", Total events: " + count + 
                   ", Memory used: " + (count * 100) + " bytes (estimated)";
        }
    }
}
