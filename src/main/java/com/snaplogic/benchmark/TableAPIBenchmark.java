package com.snaplogic.benchmark;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;

import java.io.IOException;

public class TableAPIBenchmark {

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
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        CsvTableSource csvSource = CsvTableSource
                .builder()
                .path("/Users/dchen/GitRepo/snaplogic/Snap-document/FlinkImpl/src/main/resources/test_5m.csv")
                .field("DRGDefinition", Types.STRING())
                .field("ProviderId", Types.INT())
                .field("ProviderName", Types.STRING())
                .field("ProviderStreetAddress", Types.STRING())
                .field("ProviderCity", Types.STRING())
                .field("ProviderState", Types.STRING())
                .field("ProviderZipCode", Types.STRING())
                .field("HospitalReferralRegionDescription",Types.STRING())
                .field("TotalDischarges",Types.INT())
                .field("AverageCoveredCharges",Types.STRING())
                .field("AverageTotalPayments",Types.STRING())
                .field("AverageMedicarePayments",Types.STRING())
                .ignoreFirstLine()
                .quoteCharacter('"')    //string field
                .build();

        tableEnv.registerTableSource("csvTable", csvSource);
        Table result = tableEnv.scan("csvTable").filter("ProviderState === 'AL'").orderBy("ProviderCity.desc");


        result.writeToSink(new CsvTableSink(
                "BenchmarkTable.csv",
                "|",
                1,
                FileSystem.WriteMode.OVERWRITE
        ));
    }
}