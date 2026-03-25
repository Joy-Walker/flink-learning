package com.geekbang.flink.state.keyedstate;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义实现并行度为1的source，模拟产生从1开始的递增数字
 *  实现SourceFunction接口即可，只能单线程产生数据
 */
public class CustomNoParallelSource implements SourceFunction<Long> {

    private long count = 1L;

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while(isRunning){
            ctx.collect(count);
            count++;
            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
