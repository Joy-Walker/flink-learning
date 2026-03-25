package com.geekbang.flink.wordcount;

import com.geekbang.flink.source.custom.CustomNoParallelSource;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink UI 算子结构查看 Demo
 * 
 * 使用方法：
 * 1. 启动 Flink 集群（standalone）
 * 2. 运行此程序
 * 3. 访问 http://localhost:8081 查看 Flink UI
 * 4. 点击运行的 Job，查看算子图、并行度、数据量等信息
 */
public class FlinkUIOperatorDemo {

    public static void main(String[] args) throws Exception {
        // 获取 Flink 运行环境
        Configuration conf = new Configuration();
        // 启用 Web UI 并设置端口
        conf.setInteger(RestOptions.PORT, 8081);
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        
        // 设置全局并行度（可以在 UI 上看到多个并行子任务）
        env.setParallelism(1);
        env.disableOperatorChaining();

        // 创建测试数据流，展示完整的算子链
        // source -> map -> keyby -> sum -> map -> print
        env.addSource(new CustomNoParallelSource())
            .map(value -> {
                // Map 算子：将 Long 转换为 Tuple2<(数字 % 3), 数字>
                // 这样会按 0,1,2 循环分组，可以看到每个分组的累加效果
                return Tuple2.of(value % 3, value);
            }).returns(TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){}))
            .keyBy(value -> value.f0)  // KeyBy 算子：按第一个字段（余数）分组
            .sum(1).name("sum算子")                  // Sum 算子：对第二个字段（实际数值）求和
            .map(result -> "分组 " + result.f0 + " 累加和：" + result.f1)
            .print();




        // 执行作业
        String jobName = FlinkUIOperatorDemo.class.getSimpleName();
        System.out.println("正在提交作业：" + jobName);
        System.out.println("请访问 Flink UI: http://localhost:8081");
        System.out.println("在 UI 上可以查看：");
        System.out.println("  - 算子拓扑图");
        System.out.println("  - 每个算子的并行度");
        System.out.println("  - 数据处理量（records in/out）");
        System.out.println("  - 反压情况（Backpressure）");
        System.out.println("  - 水位线（Watermark）");
        
        env.execute(jobName);
    }


}
