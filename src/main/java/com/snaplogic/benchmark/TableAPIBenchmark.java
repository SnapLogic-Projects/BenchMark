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

    public static class InputCSV {
        public String dRGDefinition;
        public int providerId;
        public String providerName;
        public String providerStreetAddress;
        public String providerCity;
        public String providerState;
        public String providerZipCode;
        public String hospitalReferralRegionDescription;
        public int totalDischarges;
        public String averageCoveredCharges;
        public String averageTotalPayments;
        public String averageMedicarePayments;

        public InputCSV() {

        }

        public InputCSV(String dRGDefinition, int providerId, String providerName, String providerStreetAddress,
                        String providerCity, String providerState, String providerZipCode, String hospitalReferralRegionDescription,
                        int totalDischarges, String averageCoveredCharges, String averageTotalPayments, String averageMedicarePayments) {
            this.dRGDefinition = dRGDefinition;
            this.providerId = providerId;
            this.providerName = providerName;
            this.providerStreetAddress = providerStreetAddress;
            this.providerCity = providerCity;
            this.providerState = providerState;
            this.providerZipCode = providerZipCode;
            this.hospitalReferralRegionDescription = hospitalReferralRegionDescription;
            this.totalDischarges = totalDischarges;
            this.averageCoveredCharges = averageCoveredCharges;
            this.averageTotalPayments = averageTotalPayments;
            this.averageMedicarePayments = averageMedicarePayments;
        }

        public int getProviderId() {
            return providerId;
        }

        public int getTotalDischarges() {
            return totalDischarges;
        }

        public String getAverageCoveredCharges() {
            return averageCoveredCharges;
        }

        public String getAverageMedicarePayments() {
            return averageMedicarePayments;
        }

        public String getAverageTotalPayments() {
            return averageTotalPayments;
        }

        public String getdRGDefinition() {
            return dRGDefinition;
        }

        public String getHospitalReferralRegionDescription() {
            return hospitalReferralRegionDescription;
        }

        public String getProviderCity() {
            return providerCity;
        }

        public String getProviderName() {
            return providerName;
        }

        public String getProviderState() {
            return providerState;
        }

        public String getProviderStreetAddress() {
            return providerStreetAddress;
        }

        public String getProviderZipCode() {
            return providerZipCode;
        }

        public void setAverageCoveredCharges(String averageCoveredCharges) {
            this.averageCoveredCharges = averageCoveredCharges;
        }

        public void setAverageMedicarePayments(String averageMedicarePayments) {
            this.averageMedicarePayments = averageMedicarePayments;
        }

        public void setAverageTotalPayments(String averageTotalPayments) {
            this.averageTotalPayments = averageTotalPayments;
        }

        public void setdRGDefinition(String dRGDefinition) {
            this.dRGDefinition = dRGDefinition;
        }

        public void setHospitalReferralRegionDescription(String hospitalReferralRegionDescription) {
            this.hospitalReferralRegionDescription = hospitalReferralRegionDescription;
        }

        public void setProviderCity(String providerCity) {
            this.providerCity = providerCity;
        }

        public void setProviderId(int providerId) {
            this.providerId = providerId;
        }

        public void setProviderName(String providerName) {
            this.providerName = providerName;
        }

        public void setProviderState(String providerState) {
            this.providerState = providerState;
        }

        public void setProviderStreetAddress(String providerStreetAddress) {
            this.providerStreetAddress = providerStreetAddress;
        }

        public void setProviderZipCode(String providerZipCode) {
            this.providerZipCode = providerZipCode;
        }

        public void setTotalDischarges(int totalDischarges) {
            this.totalDischarges = totalDischarges;
        }
    }

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

        DataSet<InputCSV> csvInput = env.readCsvFile(testfile)
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .pojoType(InputCSV.class, "dRGDefinition", "providerId", "providerName", "providerStreetAddress",
                        "providerCity", "providerState", "providerZipCode", "hospitalReferralRegionDescription",
                        "totalDischarges", "averageCoveredCharges", "averageTotalPayments", "averageMedicarePayments");

//        DataSet<Tuple12<String, Integer, String, String, String, String, String, String, Integer, String, String, String>> csvInput
//                = env.readCsvFile(testfile)
//                .ignoreFirstLine()
//                .parseQuotedStrings('"')
//                .types(String.class, Integer.class, String.class, String.class, String.class, String.class, String.class, String.class,
//                        Integer.class, String.class, String.class, String.class);

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

        Table csvTable = tableEnv.fromDataSet(csvInput);
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