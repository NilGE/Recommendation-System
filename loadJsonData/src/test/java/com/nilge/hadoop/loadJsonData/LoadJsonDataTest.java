package com.nilge.hadoop.loadJsonData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
        InfoWritable info1 = new InfoWritable();
        info1.SetName("Bike Doctor");
        info1.SetStars("5.0");
        mapDriver.withInput(new LongWritable(), new Text("{\"business_id\": \"rncjoVoEFUJGCUoC1JgnUA\", \"full_address\": \"8466 W Peoria Ave\\nSte 6\\nPeoria, AZ 85345\", \"open\": true, \"categories\": [\"Accountants\", \"Professional Services\", \"Tax Services\", \"Financial Services\"], \"city\": \"Peoria\", \"review_count\": 3, \"name\": \"Peoria Income Tax Service\", \"neighborhoods\": [], \"longitude\": -112.241596, \"state\": \"AZ\", \"stars\": 5.0, \"latitude\": 33.581867000000003, \"type\": \"business\"}\n{\"business_id\": \"0FNFSzCFP_rGUoJx8W7tJg\", \"full_address\": \"2149 W Wood Dr\\nPhoenix, AZ 85029\", \"open\": true, \"categories\": [\"Sporting Goods\", \"Bikes\", \"Shopping\"], \"city\": \"Phoenix\", \"review_count\": 5, \"name\": \"Bike Doctor\", \"neighborhoods\": [], \"longitude\": -112.10593299999999, \"state\": \"AZ\", \"stars\": 5.0, \"latitude\": 33.604053999999998, \"type\": \"business\"}"));
        mapDriver.withOutput(new Text("Peoria"), info);
        mapDriver.withOutput(new Text("Phoenix"), info1);
        mapDriver.runTest();
    }
    
    @Test
    public void testReducer() throws IOException {
        List<InfoWritable> values = new ArrayList<InfoWritable>();
        InfoWritable info1 = new InfoWritable();
        info1.SetName("name1");
        info1.SetStars("5.0");
        InfoWritable info2 = new InfoWritable();
        info2.SetName("name2");
        info2.SetStars("4.9");
        values.add(info1);
        values.add(info2);
        reduceDriver.withInput(new Text("Los Angeles"), values);
        reduceDriver.withOutput(new Text("Los Angeles"), new Text("5.0"));
        reduceDriver.withOutput(new Text("Los Angeles"), new Text("4.9"));
    }
}
