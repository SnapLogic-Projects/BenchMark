package com.snaplogic.benchmark;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.snaplogic.ExecutionException;
import com.snaplogic.common.expressions.ScopeStack;
import com.snaplogic.Document;
import com.snaplogic.DocumentImpl;
import com.snaplogic.expression.ExpressionUtil;
import com.snaplogic.expression.GlobalScope;
import com.snaplogic.expression.SnapLogicExpression;
import com.snaplogic.snap.api.SnapDataException;
import com.snaplogic.util.DefaultValueHandler;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple12;
import row.SnapRow;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

public class ExpressionBenchmark {

    private static final LoadingCache<String, SnapLogicExpression> PARSE_TREE_CACHE =
            CacheBuilder.newBuilder()
                    .softValues()
                    .build(new CacheLoader<String, SnapLogicExpression>() {
                        @Override
                        public SnapLogicExpression load(final String key) throws Exception {
                            return ExpressionUtil.compile(key);
                        }
                    });
    private static final GlobalScope GLOBAL_SCOPE = new GlobalScope();
    private static final DefaultValueHandler DEFAULT_VALUE_HANDLER = new DefaultValueHandler();
    private static final String expression = "$5 == 'AL'";
    private static SnapLogicExpression snapLogicExpression;

    public static void main(String[] args) throws Exception {

        // get flink environment.
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ScopeStack scopeStack = ExpressionEnv.InitializeEnvData(new HashMap<String, Object>());

        // warn up
        for (int i = 0; i < 1; i++) {

            process(env, scopeStack);
            try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < 1; i++) {

            process(env, scopeStack);
            try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static void process(ExecutionEnvironment env, final ScopeStack scopes) throws IOException, java.util.concurrent.ExecutionException {

        snapLogicExpression = PARSE_TREE_CACHE.get(expression);
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

//        SnapFilter snapFilter = new SnapFilter(scopes, snapLogicExpression);
//        DataSet<Document> filterOut = parsedSet.filter(snapFilter);

        // Filter Snap
        DataSet<Document> filterOut = parsedSet.filter(new FilterFunction<Document>() {
            @Override
            public boolean filter(Document document) throws Exception {
                SnapLogicExpression snapLogicExpression = PARSE_TREE_CACHE.get(expression);

                ScopeStack scopeStack;
                if (scopes != null && scopes.getClass() == ScopeStack.class) {
                    scopeStack = (ScopeStack) scopes;
                } else {
                    scopeStack = new ScopeStack();
                    if (scopes != null) {
                        scopeStack.pushAllScopes(scopes);
                    } else {
                        scopeStack.push(GLOBAL_SCOPE);
                    }
                }
                try {
                    return (Boolean) snapLogicExpression.evaluate(document.get(), scopeStack, DEFAULT_VALUE_HANDLER);
                } catch (SnapDataException |ExecutionException e) {
                    throw e;
                } catch (Throwable th) {
                    throw new SnapDataException(th, "Unexpected error occurred while " +
                            "evaluating expression: %s")
                            .formatWith(expression)
                            .withResolution("Please check your expression");
                }
            }

        });

        // Sort Snap
        DataSet<Document> sortOut = filterOut.sortPartition(new KeySelector<Document, String>() {
            @Override
            public String getKey(Document document) throws Exception {
                return (String) ((Map<String, Object>) document.get()).get("0");
            }
        }, Order.ASCENDING).setParallelism(1);

        // Writer Snap

        sortOut.writeAsFormattedText("ExpressionBenchmark.csv", OVERWRITE,
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
                                + record.get("11") ;
                    }
                }
        ).setParallelism(1);
    }

}