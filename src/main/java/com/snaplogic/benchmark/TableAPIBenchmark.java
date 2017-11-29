package com.snaplogic.benchmark;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.*;
import org.apache.flink.table.sinks.CsvTableSink;

import java.io.IOException;

public class TableAPIBenchmark {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // warn up
        for (int i = 0; i < 1; i++) {

            process(env, args[0], args[1]);
            try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        env.startNewSession();

        for (int i = 0; i < 1; i++) {

            process(env, args[0], args[1]);
            try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static void process(ExecutionEnvironment env, String testfile, String outputPath) throws IOException {
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        DataSet<Tuple12<String, Integer, String, String, String, String, String, String, Integer, String, String, String>> csvInput
                = env.readCsvFile(testfile)
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .types(String.class, Integer.class, String.class, String.class, String.class, String.class, String.class, String.class,
                        Integer.class, String.class, String.class, String.class);

//        CsvTableSource csvSource = CsvTableSource
//                .builder()
//                .path("/Users/dchen/GitRepo/snaplogic/Snap-document/FlinkImpl/src/main/resources/test_5m.csv")
//                .field("DRGDefinition", Types.STRING())
//                .field("ProviderId", Types.INT())
//                .field("ProviderName", Types.STRING())
//                .field("ProviderStreetAddress", Types.STRING())
//                .field("ProviderCity", Types.STRING())
//                .field("ProviderState", Types.STRING())
//                .field("ProviderZipCode", Types.STRING())
//                .field("HospitalReferralRegionDescription",Types.STRING())
//                .field("TotalDischarges",Types.INT())
//                .field("AverageCoveredCharges",Types.STRING())
//                .field("AverageTotalPayments",Types.STRING())
//                .field("AverageMedicarePayments",Types.STRING())
//                .ignoreFirstLine()
//                .quoteCharacter('"')    //string field
//                .build();
//
//        tableEnv.registerTableSource("csvTable", csvSource);
//        Table csvTable = tableEnv.scan("csvTable");

        Table csvTable = tableEnv.fromDataSet(csvInput, "dRGDefinition, providerId, providerName, providerStreetAddress," +
                "providerCity, providerState, providerZipCode, hospitalReferralRegionDescription," +
                "totalDischarges, averageCoveredCharges, averageTotalPayments, averageMedicarePayments");
        Table result = csvTable.where("providerState === 'AL'")
                .orderBy("dRGDefinition.asc");


        result.writeToSink(new CsvTableSink(
                outputPath,
                "|",
                1,
                FileSystem.WriteMode.OVERWRITE
        ));
    }
}