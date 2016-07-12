package com.nilge.hadoop.loadJsonData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.matchers.EndsWith;

import com.nilge.hadoop.loadJsonData.mapreduce.LoadJsonData.JsonMapper;
import com.nilge.hadoop.loadJsonData.mapreduce.LoadJsonData.JsonReducer;
import com.nilge.hadoop.loadJsonData.property.InfoWritable;
import com.sun.tools.classfile.InnerClasses_attribute.Info;

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
        mapDriver.withInput(new LongWritable(), new Text("{\"business_id\": \"rncjoVoEFUJGCUoC1JgnUA\", \"full_address\": \"8466 W Peoria Ave\nSte 6\nPeoria, AZ 85345\", \"open\": true, \"categories\": [\"Accountants\", \"Professional Services\", \"Tax Services\", \"Financial Services\"], \"city\": \"Peoria\", \"review_count\": 3, \"name\": \"Peoria Income Tax Service\", \"neighborhoods\": [], \"longitude\": -112.241596, \"state\": \"AZ\", \"stars\": 5.0, \"latitude\": 33.581867000000003, \"type\": \"business\"}\n{\"business_id\": \"0FNFSzCFP_rGUoJx8W7tJg\", \"full_address\": \"2149 W Wood Dr\nPhoenix, AZ 85029\", \"open\": true, \"categories\": [\"Sporting Goods\", \"Bikes\", \"Shopping\"], \"city\": \"Phoenix\", \"review_count\": 5, \"name\": \"Bike Doctor\", \"neighborhoods\": [], \"longitude\": -112.10593299999999, \"state\": \"AZ\", \"stars\": 5.0, \"latitude\": 33.604053999999998, \"type\": \"business\"}\n{\"business_id\": \"3f_lyB6vFK48ukH6ScvLHg\", \"full_address\": \"1134 N Central Ave\nPhoenix, AZ 85004\", \"open\": true, \"categories\": [], \"city\": \"Phoenix\", \"review_count\": 4, \"name\": \"Valley Permaculture Alliance\", \"neighborhoods\": [], \"longitude\": -112.07393329999999, \"state\": \"AZ\", \"stars\": 5.0, \"latitude\": 33.460525799999999, \"type\": \"business\"}\n{\"business_id\": \"usAsSV36QmUej8--yvN-dg\", \"full_address\": \"845 W Southern Ave\nPhoenix, AZ 85041\", \"open\": true, \"categories\": [\"Food\", \"Grocery\"], \"city\": \"Phoenix\", \"review_count\": 5, \"name\": \"Food City\", \"neighborhoods\": [], \"longitude\": -112.0853773, \"state\": \"AZ\", \"stars\": 3.5, \"latitude\": 33.392209899999997, \"type\": \"business\"}\n{\"business_id\": \"PzOqRohWw7F7YEPBz6AubA\", \"full_address\": \"6520 W Happy Valley Rd\nSte 101\nGlendale Az, AZ 85310\", \"open\": true, \"categories\": [\"Food\", \"Bagels\", \"Delis\", \"Restaurants\"], \"city\": \"Glendale Az\", \"review_count\": 14, \"name\": \"Hot Bagels & Deli\", \"neighborhoods\": [], \"longitude\": -112.200264, \"state\": \"AZ\", \"stars\": 3.5, \"latitude\": 33.712797000000002, \"type\": \"business\"}\n{\"business_id\": \"gtQzAiy7D-dPU8WzT3jX3Q\", \"full_address\": \"The Americana at Brand\n869 Americana Way\nGlendale, CA 91210\", \"open\": true, \"categories\": [\"Women's Clothing\", \"Fashion\", \"Shopping\"], \"city\": \"Glendale\", \"review_count\": 6, \"name\": \"Barney's New York Co-op\", \"neighborhoods\": [], \"longitude\": -112.481632232666, \"state\": \"CA\", \"stars\": 4.5, \"latitude\": 33.6077660588006, \"type\": \"business\"}\n{\"business_id\": \"FrBCYtCS_jydDjg1KsIgWQ\", \"full_address\": \"1850 W Camelback Rd\nSte 1\nPhoenix, AZ 85015\", \"open\": true, \"categories\": [\"Music & DVDs\", \"Books, Mags, Music & Video\", \"Vinyl Records\", \"Shopping\"], \"city\": \"Phoenix\", \"review_count\": 21, \"name\": \"Zia Record Exchange\", \"neighborhoods\": [], \"longitude\": -112.0987501, \"state\": \"AZ\", \"stars\": 3.5, \"latitude\": 33.509658199999997, \"type\": \"business\"}\n{\"business_id\": \"yaXAD-Mv2K2PEZobmqjIYA\", \"full_address\": \"3415 W Northern Ave\nPhoenix, AZ 85051\", \"open\": true, \"categories\": [\"Event Planning & Services\", \"Venues & Event Spaces\"], \"city\": \"Phoenix\", \"review_count\": 4, \"name\": \"Reflections Bingo\", \"neighborhoods\": [], \"longitude\": -112.13286600000001, \"state\": \"AZ\", \"stars\": 5.0, \"latitude\": 33.551903000000003, \"type\": \"business\"}\n{\"business_id\": \"o3ehs4ZEdsizbJyB9_j7uQ\", \"full_address\": \"6938 E 1st St\nScottsdale, AZ 85251\", \"open\": true, \"categories\": [\"Art Schools\", \"Specialty Schools\", \"Shopping\", \"Jewelry\", \"Accessories\", \"Fashion\", \"Education\"], \"city\": \"Scottsdale\", \"review_count\": 14, \"name\": \"Jam\", \"neighborhoods\": [], \"longitude\": -111.9312992, \"state\": \"AZ\", \"stars\": 5.0, \"latitude\": 33.492195000000002, \"type\": \"business\"}\n{\"business_id\": \"qarobAbxGSHI7ygf1f7a_Q\", \"full_address\": \"891 E Baseline Rd\nSuite 102\nGilbert, AZ 85233\", \"open\": true, \"categories\": [\"Sandwiches\", \"Restaurants\"], \"city\": \"Gilbert\", \"review_count\": 10, \"name\": \"Jersey Mike's Subs\", \"neighborhoods\": [], \"longitude\": -111.8120071, \"state\": \"AZ\", \"stars\": 3.5, \"latitude\": 33.378838500000001, \"type\": \"business\"}\n"));
        mapDriver.withOutput(new Text("Peoria"), info);
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
