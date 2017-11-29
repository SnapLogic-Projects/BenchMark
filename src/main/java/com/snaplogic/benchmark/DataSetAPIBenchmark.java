package com.snaplogic.benchmark;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple12;

import java.io.IOException;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

public class DataSetAPIBenchmark {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // warn up
        for (int i = 0; i < 1; i++) {

            process(env);
            try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        env.startNewSession();

        for (int i = 0; i < 1; i++) {

            process(env);
            try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static void process(ExecutionEnvironment env) throws IOException {
        DataSet<Tuple12<String, Integer, String, String, String, String, String, String, Integer, String, String, String>> csvInput
                = env.readCsvFile("/Users/dchen/GitRepo/snaplogic/Snap-document/FlinkImpl/src/main/resources/test_5m.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .types(String.class, Integer.class, String.class, String.class, String.class, String.class, String.class, String.class,
                        Integer.class, String.class, String.class, String.class);

        DataSet<Tuple12<String, Integer, String, String, String, String, String, String, Integer, String, String, String>> output0
                = csvInput.filter(new FilterFunction<Tuple12<String, Integer, String, String, String, String, String, String, Integer, String, String, String>>() {
            @Override
            public boolean filter(Tuple12<String, Integer, String, String, String, String, String, String, Integer, String, String, String> input) throws Exception {
                return input.f5.equals("AL");
            }
        }).sortPartition(0, Order.ASCENDING)
                .setParallelism(1);

        output0.writeAsCsv("DataSetAPIBenchmark.csv", "\n", "|", OVERWRITE).setParallelism(1);
    }

}