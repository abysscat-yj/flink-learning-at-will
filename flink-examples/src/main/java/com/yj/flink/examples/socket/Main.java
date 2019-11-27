package com.yj.flink.examples.socket;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: yuanj
 * @Date: 2019/11/27 16:05
 */
public class Main {

    public static void main(String[] args) throws Exception {

        // 参数检查
        if (args.length != 2) {
            System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
            return;
        }

        String hostname = args[0];
        Integer port = Integer.parseInt(args[1]);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.socketTextStream(hostname, port);

        stream.flatMap(new LineSplitter())
                .keyBy(0)
                .sum(1)
                .print();

        env.execute("yj's socket wordcount demo");


    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] splits = s.toLowerCase().split("\\W+");

            for (String split : splits) {
                if (split.length() > 0) {
                    collector.collect(new Tuple2<String, Integer>(split, 1));
                }
            }
        }
    }

}
