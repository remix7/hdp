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
public class InverIndexSetupTwo {
    static class InverIndexSetupTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text k = new Text();
        IntWritable v = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] name_info = line.split("--");
            context.write(new Text(name_info[0]), new Text(name_info[1]));

        }
    }

    static class InverIndexSetupTwoReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Text value : values) {
                sb.append(value);
            }
            context.write(key, new Text(sb.toString()));
        }
    }
}
