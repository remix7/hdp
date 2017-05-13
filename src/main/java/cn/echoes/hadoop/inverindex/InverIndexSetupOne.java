package cn.echoes.hadoop.inverindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * -------------------------------------
 * TODO
 * -------------------------------------
 * Created by liutao on 2017/5/9 23:29.
 */
public class InverIndexSetupOne {
    static class InverIndexSetupOneMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        Text k = new Text();
        IntWritable v = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            FileSplit split = (FileSplit) context.getInputSplit();
            String name = split.getPath().getName();
            for(String word : words){
                k.set(word+"---"+name);
                context.write(k,v);
            }

        }
    }

    static class InverIndexSetupOneReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable value :values){
                count += value.get();
            }
            context.write(key,new IntWritable(count));
        }
    }
}
