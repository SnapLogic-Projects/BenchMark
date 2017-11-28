package com.snaplogic.benchmark;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.snaplogic.api.ExecutionException;
import com.snaplogic.common.expressions.ScopeStack;
import com.snaplogic.expression.ExpressionUtil;
import com.snaplogic.expression.GlobalScope;
import com.snaplogic.expression.SnapLogicExpression;
import com.snaplogic.snap.api.SnapDataException;
import com.snaplogic.util.DefaultValueHandler;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextOutputFormat;

import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import row.SnapRow;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

public class SnapRowBenchmark {

    private static Map<String, Integer> fieldMap = new HashMap<>();

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
    private static final String expression = "$ProviderState == 'AL'";
    private static SnapLogicExpression snapLogicExpression;

    public static void main(String[] args) throws java.util.concurrent.ExecutionException {

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


    public static void process(ExecutionEnvironment env, final ScopeStack scopes) throws java.util.concurrent.ExecutionException {
        snapLogicExpression = PARSE_TREE_CACHE.get(expression);
        // parse header
        String headStr = "DRGDefinition,ProviderId,ProviderName,ProviderStreetAddress,ProviderCity," +
                "ProviderState,ProviderZipCode,HospitalReferralRegionDescription, TotalDischarges , " +
                "AverageCoveredCharges , AverageTotalPayments ,AverageMedicarePayments";
        String[] header = headStr.split(",");
        String[] fieldNames = new String[header.length];
        for (int i = 0; i < header.length; i++) {
            fieldMap.put(header[i].trim(), i);
            fieldNames[i] = header[i].trim();
        }
        TypeInformation<?>[] fieldTypes = {BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO};

        CsvTableSource csvTableSource = new CsvTableSource("/Users/dchen/GitRepo/snaplogic/Snap-document/FlinkImpl/src/main/resources/test_5m.csv", fieldNames, fieldTypes,
                ",", "\n", '"', true, null, false);

        DataSet<Row> dataSet = csvTableSource.getDataSet(env);
        Row2SnapRow mapper = new Row2SnapRow();
        DataSet<SnapRow> snapRowDataSet = dataSet.map(mapper);

//        SnapFilter snapFilter = new SnapFilter(scopes, snapLogicExpression);
//        DataSet<SnapRow> filtered = snapRowDataSet.filter(snapFilter);

        DataSet<SnapRow> filtered = snapRowDataSet.filter(new FilterFunction<SnapRow>() {
            @Override
            public boolean filter(SnapRow value) throws Exception {
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
                    return (boolean)snapLogicExpression.evaluate(value, scopeStack, DEFAULT_VALUE_HANDLER);
                } catch (SnapDataException|ExecutionException e) {
                    throw e;
                } catch (Throwable th) {
                    throw new SnapDataException(th, "Unexpected error occurred while " +
                            "evaluating expression: %s")
                            .formatWith(expression)
                            .withResolution("Please check your expression");
                }
            }
        });


        DataSet<SnapRow> sorted = filtered.sortPartition(new KeySelector<SnapRow, String>() {
            @Override
            public String getKey(SnapRow value) throws Exception {
                return (String) value.getField("ProviderCity");
            }
        }, Order.DESCENDING).setParallelism(1);

        sorted.writeAsFormattedText("BenchmarkSnapRow.csv", OVERWRITE,
                new TextOutputFormat.TextFormatter<SnapRow>() {
                    @Override
                    public String format(SnapRow record) {
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
                                + record.getField(11) + "|";
                    }
                }
        ).setParallelism(1);
    }

    private static class Row2SnapRow implements MapFunction<Row, SnapRow> {
        @Override
        public SnapRow map(Row row) throws Exception {
            SnapRow snapRow = new SnapRow(row.getArity(), fieldMap);
            for (int i = 0; i < row.getArity(); i++) {
                snapRow.setField( i, row.getField(i));
            }
            return snapRow;
        }
    }

//    private static class SnapFilter implements FilterFunction<SnapRow> {
//
//        ScopeStack scopes;
//        SnapLogicExpression snapLogicExpression;
//
//        public SnapFilter(ScopeStack scopeStack, SnapLogicExpression expression) {
//            this.scopes = scopeStack;
//            this.snapLogicExpression = expression;
//        }
//
//        @Override
//        public boolean filter(SnapRow value) throws Exception {
//            ScopeStack scopeStack;
//            if (scopes != null && scopes.getClass() == ScopeStack.class) {
//                scopeStack = (ScopeStack) scopes;
//            } else {
//                scopeStack = new ScopeStack();
//                if (scopes != null) {
//                    scopeStack.pushAllScopes(scopes);
//                } else {
//                    scopeStack.push(GLOBAL_SCOPE);
//                }
//            }
//            try {
//                if (this.snapLogicExpression == null)
//                    return true;
//                else
//                    return (boolean)this.snapLogicExpression.evaluate(value, scopeStack, DEFAULT_VALUE_HANDLER);
//            } catch (SnapDataException|ExecutionException e) {
//                throw e;
//            } catch (Throwable th) {
//                throw new SnapDataException(th, "Unexpected error occurred while " +
//                        "evaluating expression: %s")
//                        .formatWith(expression)
//                        .withResolution("Please check your expression");
//            }
//        }
//    }
}
