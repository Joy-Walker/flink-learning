package com.geekbang.flink.state.operatorstate;

import com.geekbang.flink.state.keyedstate.CustomNoParallelSource;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class MyCheckPointedFunction implements CheckpointedFunction, SinkFunction<String> {

    List<String> buffer;

    int threshold;

    // 每个子任务独立维护一个状态
    ListState<String> listState;

    // 在恢复时，会将所有子任务的状态进行合并
    ListState<String> unionListState;
    public MyCheckPointedFunction(int threshold) {
        this.threshold = threshold;
        this.buffer = new ArrayList<>();
    }


    @Override
    public void invoke(String value, Context context) throws Exception {
        buffer.add(value);
        if (buffer.size() == threshold) {
            // sink the data
            System.out.println(buffer);
            buffer.clear();
        }
    }

    /**
     * 创建一个快照,在创建快照时，Flink会调用此方法。
     * 需要将内存中的数据存储到状态中，flink会自动对其进行持久化。
     * @param context the context for drawing a snapshot of the operator
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("snapshotState 被调用");
        listState.clear();  // 清除之前的快照数据,因为每次都会把buffer中的全量数据都写入一遍
        for (String element : buffer) {
            listState.add(element);
        }
    }


    /**
     *
     /**
     * 此方法在分布式执行期间创建并行函数实例时被调用。
     * 函数通常在此方法中设置其状态存储数据结构。
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("initializeState 被调用");
        listState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("buffer", String.class));
        unionListState = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<>("buffer", String.class));
        // 如果是从状态中恢复数据
        if(context.isRestored()) {
            for (String element : listState.get()) {
                buffer.add(element);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        MyCheckPointedFunction myCheckPointedFunction = new MyCheckPointedFunction(100);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE);
        env.addSource(new CustomNoParallelSource()).map(String::valueOf).addSink(myCheckPointedFunction);
        env.execute();
    }
}
