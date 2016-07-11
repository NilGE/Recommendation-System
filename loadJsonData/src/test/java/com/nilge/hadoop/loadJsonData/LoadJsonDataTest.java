package com.nilge.hadoop.loadJsonData;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.nilge.hadoop.loadJsonData.mapreduce.LoadJsonData.JsonMapper;
import com.nilge.hadoop.loadJsonData.mapreduce.LoadJsonData.JsonReducer;
import com.nilge.hadoop.loadJsonData.property.InfoWritable;

public class LoadJsonDataTest {
    MapDriver<Object, Text, Text, InfoWritable> mapDriver;
    ReduceDriver<Text, InfoWritable, Text, Text> reduceDriver;
    MapReduceDriver<Object, Text, Text, InfoWritable, Text, Text> mapReduceDriver;
    
    @Before
    public void setUp() {
        JsonMapper mapper = new JsonMapper();
        JsonReducer reducer = new JsonReducer();
        
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }
    
    @Test
    public void testMapper() throws IOException {
        InfoWritable info = new InfoWritable();
        info.SetName("Peoria Income Tax Service");
        info.SetStars("5.0");
        mapDriver.withInput(new LongWritable(), new Text("{\"business_id\": \"rncjoVoEFUJGCUoC1JgnUA\", "
                + "\"full_address\": \"8466 W Peoria Ave\nSte 6\nPeoria, AZ 85345\", \"open\": true, "
                + "\"categories\": [\"Accountants\", \"Professional Services\", \"Tax Services\", \"Financial Services\"], "
                + "\"city\": \"Peoria\", \"review_count\": 3, \"name\": \"Peoria Income Tax Service\", \"neighborhoods\": [], "
                + "\"longitude\": -112.241596, \"state\": \"AZ\", \"stars\": 5.0, \"latitude\": 33.581867000000003, "
                + "\"type\": \"business\"}"));
        mapDriver.withOutput(new Text("Peoria"), info);
        mapDriver.runTest();
    }
}
