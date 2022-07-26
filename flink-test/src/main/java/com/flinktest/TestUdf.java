package com.flinktest;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDateTime;

public class TestUdf extends  ScalarFunction {
    @DataTypeHint("TIMESTAMP(3)")
    public LocalDateTime eval(String strDate) {
       return LocalDateTime.now();
    }
}
