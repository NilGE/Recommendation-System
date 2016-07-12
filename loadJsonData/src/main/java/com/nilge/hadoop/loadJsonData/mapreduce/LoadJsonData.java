package com.nilge.hadoop.loadJsonData.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.nilge.hadoop.loadJsonData.property.Business;
import com.nilge.hadoop.loadJsonData.property.InfoWritable;



public class LoadJsonData {
    public static class JsonMapper extends Mapper<Object, Text, Text, InfoWritable> {
        private final InfoWritable info = new InfoWritable();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            try {
                String[] tuples = value.toString().split("\\n");
                Gson gson = new Gson();
                for (String tuple : tuples) {
                    JsonReader reader = new JsonReader(new StringReader(tuple));
                    reader.setLenient(true);
                    Business business = gson.fromJson(reader, Business.class);
                    info.SetName(business.name);
                    info.SetStars(business.stars);
                    context.write(new Text(business.city), info);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public static class JsonReducer extends Reducer<Text, InfoWritable, Text, Text> {
        private TreeMap<Text, Text> tree = new TreeMap<Text, Text>();
        public void reduce(Text key, Iterable<InfoWritable> values, Context context) throws IOException, InterruptedException {
            for (InfoWritable info : values) {
                tree.put(new Text(info.getName()), new Text(info.getStars()));
            }
            for (Text t : tree.values()) {
                context.write(key, t);
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        runJob(args[0], args[1]);
    }
    
    public static void runJob(String input, String output) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "load json data");
        job.setJarByClass(LoadJsonData.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InfoWritable.class);
        job.setMapperClass(JsonMapper.class);
        job.setReducerClass(JsonReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job, new Path(input));
        Path outPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);
        
        job.waitForCompletion(true);
    }
}
