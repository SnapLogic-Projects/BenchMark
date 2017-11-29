package com.snaplogic.benchmark;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

public class RowBenchmark {

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

    private static class Map2Row implements MapFunction<Map<String, Object>, Row> {

        @Override
        public Row map(Map<String, Object> record) throws Exception {
            Row row = new Row(record.size());

            int index = 0;
            for (String field : record.keySet()) {
                row.setField(index, record.get(field));
                index++;
            }
            return row;
        }
    }

    static void process(ExecutionEnvironment env, String testfile, String outputPath) throws IOException {

        String[] fieldNames = {"DRGDefinition", "ProviderId", "ProviderName", "ProviderStreetAddress", "ProviderCity",
                "ProviderState", "ProviderZipCode", "HospitalReferralRegionDescription", "TotalDischarges",
                "AverageCoveredCharges", "AverageTotalPayments", "AverageMedicarePayments"};

        TypeInformation<?>[] fieldTypes = {BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO};

        CsvTableSource csvTableSource = new CsvTableSource(testfile, fieldNames, fieldTypes,
                ",", "\n", '"', true, null, false);

        DataSet<Row> dataSet = csvTableSource.getDataSet(env);
        DataSet<Row> filtered = dataSet.filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row value) throws Exception {
                return value.getField(5).equals("AL");
            }
        });

        DataSet<Row> sorted = filtered
                .partitionByRange(0).withOrders(Order.ASCENDING)
                .sortPartition(new KeySelector<Row, String>() {
            @Override
            public String getKey(Row value) throws Exception {
                return (String)value.getField(0);
            }
        }, Order.ASCENDING);

        sorted.writeAsFormattedText(outputPath, OVERWRITE,
                new TextOutputFormat.TextFormatter<Row>() {
                    @Override
                    public String format(Row record) {
                        return record.getField(0) + "|"
                                + record.getField(1) + "|"
                                + record.getField(2) + "|"
                                + record.getField(3) + "|"
                                + record.getField(4) + "|"
                                + record.getField(5) + "|"
                                + record.getField(6) + "|"
                                + record.getField(7) + "|"
                                + record.getField(8) + "|"
                                + record.getField(9) + "|"
                                + record.getField(10) + "|"
                                + record.getField(11);
                    }
                }
        ).setParallelism(1);
    }
}