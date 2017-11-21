package com.snaplogic.benchmark;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.snaplogic.Document;
import com.snaplogic.DocumentImpl;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

public class DocumentOnlyBenchmark {

    public static void main(String[] args) throws Exception {

        // get flink environment.
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

//        // csv Reader Snap
//        CsvMapper mapper = new CsvMapper();
//        CsvSchema schema = CsvSchema.emptySchema().withHeader();
//        File csvFile = new File("/Users/dchen/GitRepo/snaplogic/Snap-document/FlinkImpl/src/main/resources/test.csv");
//
//        System.out.println("Start loading");
//
//        MappingIterator<Map<String, Object>> iterator = mapper.reader(Map.class)
//                .with(schema)
//                .readValues(csvFile);
//
//        ArrayList<Document> list = new ArrayList<Document>();
//
//        while (iterator.hasNext()) {
//            Map<String, Object> map = iterator.next();
//            Document cur = new DocumentImpl(map);
//            list.add(cur);
//        }
//        System.out.println("Finish loading");

//        String path = "/Users/dchen/GitRepo/snaplogic/Snap-document/FlinkImpl/src/main/resources/test.csv";
//        TextInputFormat format = new TextInputFormat(new Path(path));
//
//        DataSet<String> inputSet = env.readFile(format, path);
//
//        DataSet<Document> parsedSet = inputSet.map(new MapFunction<String, Document>() {
//            @Override
//            public Document map(String s) throws Exception {
//                return null;
//            }
//        });
        DataSet<Tuple12<String, Integer, String, String, String, String, String, String, Integer, String, String, String>> csvInput
                = env.readCsvFile("/Users/dchen/GitRepo/snaplogic/Snap-document/FlinkImpl/src/main/resources/test_5m.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .types(String.class, Integer.class, String.class, String.class, String.class, String.class, String.class, String.class,
                        Integer.class, String.class, String.class, String.class);

        DataSet<Document> parsedSet = csvInput.map(new MapFunction<Tuple12<String, Integer, String, String, String, String, String, String, Integer, String, String, String>, Document>() {
            @Override
            public Document map(Tuple12<String, Integer, String, String, String, String, String, String, Integer, String, String, String> data) throws Exception {

                Map<String, Object> map = new HashMap<>();

                for (int i = 0; i < 12 ; i++) {
                    map.put(Integer.toString(i), data.getField(i));
                }

                return new DocumentImpl(map);
            }
        });

        // Filter Sna
        DataSet<Document> filterOut = parsedSet.filter(new FilterFunction<Document>() {
            @Override
            public boolean filter(Document document) throws Exception {
                return ((Map<String, Object>) document.get()).get("5").equals("AL");
            }
        });

        // Sort Snap
        DataSet<Document> sortOut = filterOut.sortPartition(new KeySelector<Document, String>() {
            @Override
            public String getKey(Document document) throws Exception {
                return (String) ((Map<String, Object>) document.get()).get("4");
            }
        }, Order.DESCENDING).setParallelism(1);

        // Writer Snap
        sortOut.writeAsFormattedText("BenchmarkWithoutExpr.csv", OVERWRITE,
                new TextOutputFormat.TextFormatter<Document>() {
                    @Override
                    public String format(Document document) {
                        Map<String, Object> record = (Map<String, Object>)document.get();
                        return record.get("0") + "|"
                                + record.get("1") + "|"
                                + record.get("2") + "|"
                                + record.get("3") + "|"
                                + record.get("4") + "|"
                                + record.get("5") + "|"
                                + record.get("6") + "|"
                                + record.get("7") + "|"
                                + record.get("8") + "|"
                                + record.get("9") + "|"
                                + record.get("10") + "|"
                                + record.get("11") + "|";
                    }
                }
        ).setParallelism(1);
    }
}