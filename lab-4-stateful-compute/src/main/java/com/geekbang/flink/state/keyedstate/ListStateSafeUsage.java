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
 * ListState 正确使用示例 - 避免 OOM
 * 
 * ✅ 正确做法：控制数据量、定期清理、使用 TTL
 */
public class ListStateSafeUsage {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        DataStreamSource<Tuple2<String, String>> source = env.fromElements(
            Tuple2.of("user1", "event1"),
            Tuple2.of("user1", "event2"),
            Tuple2.of("user1", "event3"),
            Tuple2.of("user1", "event4"),
            Tuple2.of("user1", "event5"),
            Tuple2.of("user2", "event1"),
            Tuple2.of("user2", "event2")
        );
        
        // ✅ 方案 1：限制列表大小（滑动窗口模式）
        source.keyBy(0)
              .map(new BoundedListStateFunction(3))  // 只保留最近 3 条
              .print("方案 1");
        
        // ✅ 方案 2：使用 TTL 自动过期
        // env.getConfig().setStateTtl(Time.minutes(5));
        
        env.execute("ListState Safe Usage");
    }
    
    /**
     * ✅ 安全用法：限制 ListState 的大小
     */
    static class BoundedListStateFunction extends RichMapFunction<Tuple2<String, String>, String> {
        
        private transient ListState<String> recentEventsState;
        private final int maxSize;
        
        public BoundedListStateFunction(int maxSize) {
            this.maxSize = maxSize;
        }
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<String> descriptor = new ListStateDescriptor<>(
                "recent_events",
                Types.STRING
            );
            recentEventsState = getRuntimeContext().getListState(descriptor);
        }
        
        @Override
        public String map(Tuple2<String, String> value) throws Exception {
            // 1. 获取当前列表
            List<String> events = new ArrayList<>();
            for (String event : recentEventsState.get()) {
                events.add(event);
            }
            
            // 2. 添加新事件
            events.add(value.f1);
            
            // 3. ✅ 关键：如果超过大小限制，移除最早的事件
            while (events.size() > maxSize) {
                events.remove(0);  // 移除第一个（FIFO）
            }
            
            // 4. 更新状态（覆盖整个列表）
            recentEventsState.update(events);
            
            return String.format("Key: %s, Recent %d events: %s", 
                               value.f0, events.size(), String.join(",", events));
        }
    }
}
