package com.microstrategy.realtime;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.ExecutionException;

public class FlinkTest {
    // exception:
    // https://issues.apache.org/jira/browse/FLINK-24926
    @Test
    void testJoinStream() {
        var env = StreamExecutionEnvironment.createLocalEnvironment();
        //var env = StreamExecutionEnvironment.createRemoteEnvironment("tec-l-1033883.labs.microstrategy.com", 8081);
        env.setParallelism(3);
        var tableEnv = StreamTableEnvironment.create(env);

        var testTable = tableEnv.from(TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .column("v", DataTypes.INT())
                        .watermark("ts", "ts - INTERVAL '1' second")
                        .build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 2L)
                .option("fields.v.kind", "sequence")
                .option("fields.v.start", "0")
                .option("fields.v.end", "1000000")
                .build());
        testTable.printSchema();
        tableEnv.createTemporaryView("test", testTable );

        var joined = tableEnv.sqlQuery("SELECT ts, v, v2 from test" +
                " join (SELECT ts as ts2, v as v2 from test) on ts = ts2");

        try {
            var tableResult = joined.executeInsert(TableDescriptor.forConnector("print").build());
            tableResult.await();

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
