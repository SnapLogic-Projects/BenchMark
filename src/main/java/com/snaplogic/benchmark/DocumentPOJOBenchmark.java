package com.snaplogic.benchmark;

import com.snaplogic.Document;
import com.snaplogic.DocumentImpl;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple12;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

public class DocumentPOJOBenchmark {

    public static class InputCSV {
        private String dRGDefinition;
        private int providerId;
        private String providerName;
        private String providerStreetAddress;
        private String providerCity;
        private String providerState;
        private String providerZipCode;
        private String hospitalReferralRegionDescription;
        private int totalDischarges;
        private String averageCoveredCharges;
        private String averageTotalPayments;
        private String averageMedicarePayments;

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

        // get flink environment.
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

        for (int i = 0; i < 1; i++) {

            process(env, args[0], args[1]);
            try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static void process(ExecutionEnvironment env, String testFile, String outputPath) throws IOException {

        final DataSet<POJOBenchmark.InputCSV> csvInput = env.readCsvFile(testFile)
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .pojoType(POJOBenchmark.InputCSV.class, "dRGDefinition", "providerId", "providerName", "providerStreetAddress",
                        "providerCity", "providerState", "providerZipCode", "hospitalReferralRegionDescription",
                        "totalDischarges", "averageCoveredCharges", "averageTotalPayments", "averageMedicarePayments");

        DataSet<Document> parsedSet = csvInput.map(new MapFunction<POJOBenchmark.InputCSV, Document>() {
            @Override
            public Document map(POJOBenchmark.InputCSV inputCSV) throws Exception {

                Map<String, Object> map = new HashMap<>();

                map.put("dRGDefinition", inputCSV.getdRGDefinition());
                map.put("providerId", inputCSV.getProviderId());
                map.put("providerName", inputCSV.getProviderName());
                map.put("providerStreetAddress", inputCSV.getdRGDefinition());
                map.put("providerCity", inputCSV.getProviderCity());
                map.put("providerState", inputCSV.getProviderState());
                map.put("providerZipCode", inputCSV.getProviderZipCode());
                map.put("hospitalReferralRegionDescription", inputCSV.getHospitalReferralRegionDescription());
                map.put("totalDischarges", inputCSV.getTotalDischarges());
                map.put("averageCoveredCharges", inputCSV.getAverageCoveredCharges());
                map.put("averageTotalPayments", inputCSV.getAverageMedicarePayments());
                map.put("averageMedicarePayments", inputCSV.getAverageMedicarePayments());

                return new DocumentImpl(map);
            }
        });

        // Filter Snap
        DataSet<Document> filterOut = parsedSet.filter(new FilterFunction<Document>() {
            @Override
            public boolean filter(Document document) throws Exception {
                return ((Map<String, Object>) document.get()).get("dRGDefinition").equals("AL");
            }
        });

        // Sort Snap
        DataSet<Document> sortOut = filterOut
                .partitionByRange(0).withOrders(Order.ASCENDING)
                .sortPartition(new KeySelector<Document, String>() {
            @Override
            public String getKey(Document document) throws Exception {
                return (String) ((Map<String, Object>) document.get()).get("dRGDefinition");
            }
        }, Order.ASCENDING);

        // Writer Snap
        sortOut.writeAsFormattedText(outputPath, OVERWRITE,
                new TextOutputFormat.TextFormatter<Document>() {
                    @Override
                    public String format(Document document) {
                        Map<String, Object> record = (Map<String, Object>)document.get();
                        return record.get("dRGDefinition") + "|"
                                + record.get("providerId") + "|"
                                + record.get("providerName") + "|"
                                + record.get("providerStreetAddress") + "|"
                                + record.get("providerCity") + "|"
                                + record.get("providerState") + "|"
                                + record.get("providerZipCode") + "|"
                                + record.get("hospitalReferralRegionDescription") + "|"
                                + record.get("totalDischarges") + "|"
                                + record.get("averageCoveredCharges") + "|"
                                + record.get("averageTotalPayments") + "|"
                                + record.get("averageMedicarePayments");
                    }
                }
        ).setParallelism(1);
    }
}
