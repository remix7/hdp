package cn.echoes.hadoop.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * -------------------------------------
 * TODO
 * -------------------------------------
 * Created by liutao on 2017/2/26 20:21.
 */
public class TestMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text documentId;
    private Text word = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
        documentId = new Text(filename);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        for (String token : StringUtils.split(value.toString())) {
            word.set(token);
            context.write(word, documentId);
        }
    }
}
