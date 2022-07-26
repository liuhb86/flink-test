package com.flinktest;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

public class FlinkTest {
    @Test
    void testUdf() throws Exception {
        //var env = StreamExecutionEnvironment.createLocalEnvironment();
        // run `gradlew shadowJar` first to generate the uber jar.
        // It contains the kafka connector and a dummy UDF function.
        var env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 8081,
                "build/libs/flink-test-all.jar");
        env.setParallelism(1);
        var tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporarySystemFunction("TEST_UDF", TestUdf.class);

        var testTable = tableEnv.from(TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("time_stamp", DataTypes.STRING())
                        .columnByExpression("udf_ts", "TEST_UDF(time_stamp)")
                        .watermark("udf_ts", "udf_ts - INTERVAL '1' second")
                        .build())
                // the kafka server doesn't need to exist. It fails in the compile stage before fetching data.
                .option("properties.bootstrap.servers", "localhost:9092")
                .option("topic", "test_topic")
                .option("format", "json")
                .option("scan.startup.mode", "latest-offset")
                .build());
        testTable.printSchema();
        tableEnv.createTemporaryView("test", testTable );

        var query = tableEnv.sqlQuery("select * from test");
        var tableResult = query.executeInsert(TableDescriptor.forConnector("print").build());
        tableResult.await();
    }
}
