package com.geekbang.flink.source.custom;

import org.apache.flink.api.common.accumulators.AverageAccumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 自定义实现一个支持并行度的source
 * 1. 构造函数调用（创建实例）
 * 2. open() 调用（初始化，只执行一次）
 * 3. run() 执行（持续运行）
 * 4. close() 调用（清理资源）
 * 5. cancel() 调用（如果作业被取消）
 */


/**
 * 每个子任务都有一个CustomRichParallelSource实例。且jvm隔离
 * JobManager
 *     ↓
 * Task Slot 0                    Task Slot 1
 *     │                              │
 *     └─ CustomRichParallelSource    └─ CustomRichParallelSource
 *        实例 1                         实例 2
 *
 *        - count = 1                  - count = 1
 *        - isRunning = true           - isRunning = true
 *        - open() 执行一次            - open() 执行一次
 *        - run() 独立运行             - run() 独立运行
 *
 */
public class CustomRichParallelSource extends RichParallelSourceFunction<Long> {

    private long count = 1L;

    private boolean isRunning = true;

    /**
     * 启动数据源读取线程
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        System.out.println("Executing the run method: " + "ThreadName: " + Thread.currentThread().getName());
        while(isRunning){
            ctx.collect(count);
            this.getRuntimeContext().getAccumulator("accumulator").add(count);
            //每秒产生一条数据
            count++;
            Thread.sleep(1000);
            if(count >= 10) {
                isRunning = false;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    /**
     * 只在启动过程中被调用一次,实现对Function中的状态初始化,优先于run方法执行
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("Executing the open method: " + "ThreadName: " + Thread.currentThread().getName());
        this.getRuntimeContext().addAccumulator("accumulator",new LongCounter());
        super.open(parameters);
    }

    /**
     * 实现关闭链接的代码
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
    }
}
