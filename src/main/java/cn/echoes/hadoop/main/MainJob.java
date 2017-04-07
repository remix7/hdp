package cn.echoes.hadoop.main;

import cn.echoes.hadoop.mapper.TestMapper;
import cn.echoes.hadoop.reduce.TestReduce;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.soap.Text;
import java.io.IOException;

/**
 * -------------------------------------
 * TODO
 * -------------------------------------
 * Created by liutao on 2017/2/26 20:48.
 */
public class MainJob {
    public static void main(String args[]) {

    }

    public static void runJob(String[] input, String output)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setMapperClass(TestMapper.class);
        job.setReducerClass(TestReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        Path path = new Path(output);

        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, path);

        path.getFileSystem(conf).delete(path, true);

        job.waitForCompletion(true);


    }
}
