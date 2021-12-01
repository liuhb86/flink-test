package com.flinktest;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class FlinkTest {
    // exception:
    // Window can only be defined on a time attribute column, but is type of TIMESTAMP(3)
    // https://issues.apache.org/jira/browse/FLINK-25137
    @Test
    void testAggregationAfterJoin() throws Exception {
        var env = StreamExecutionEnvironment.createLocalEnvironment();
        //var env = StreamExecutionEnvironment.createRemoteEnvironment("", 8081);
        env.setParallelism(3);
        var tableEnv = StreamTableEnvironment.create(env);

        // create a streaming table
        var testTable = tableEnv.from(TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .column("v", DataTypes.INT())
                        .watermark("ts", "ts - INTERVAL '1' second")
                        .build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 2L)
                .option("fields.v.min", "0")
                .option("fields.v.max", "2")
                .build());
        testTable.printSchema();
        tableEnv.createTemporaryView("test", testTable );

        // create a bounded lookup table
        List<Row> data = new ArrayList<>();
        data.add(Row.of(0, 0L, "Hi"));
        data.add(Row.of(1, 1L, "Hello"));
        data.add(Row.of(2, 2L, "Hello world"));
        TypeInformation<?>[] types = {
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO};
        String[] names = {"a", "b", "c"};
        var typeInfo = new RowTypeInfo(types, names);
        var ds = env.fromCollection(data).returns(typeInfo);
        Table in = tableEnv.fromDataStream(ds, "a,b,c");
        tableEnv.createTemporaryView("lookup", in);

        var joined = tableEnv.sqlQuery("SELECT test.ts, test.v, lookup.c from test" +
                " join lookup on test.v = lookup.a");
        tableEnv.createTemporaryView("joined_table", joined);
        joined.printSchema();

        var agg = tableEnv.sqlQuery("SELECT window_start, window_end, c, sum(v)\n" +
                "  FROM TABLE(\n" +
                "    HOP(TABLE joined_table, DESCRIPTOR(ts), INTERVAL '10' seconds, INTERVAL '5' minutes))\n" +
                "  GROUP BY window_start, window_end, c");
        System.out.println(agg.explain());

        var tableResult = agg.executeInsert(TableDescriptor.forConnector("print").build());
        tableResult.await();
    }
}
