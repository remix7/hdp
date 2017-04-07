package cn.echoes.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * -------------------------------------
 * TODO
 * -------------------------------------
 * Created by liutao on 2017/4/7 23:09.
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("mapreduce.framework.name","yarn");  告诉mapreduce  yarn在哪 并且以yarn执行
//        conf.set("yarn.resourcemanage.hostname","hostname");
        Job job = Job.getInstance(conf);
        //设置jar包的路径
        job.setJarByClass(WordCountDriver.class);
        //设置任务mapper reducer 类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置reduce 输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置输入目录
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        //设置输出目录
        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));
        //将任务提交给yarn
//        job.submit();  这种方式不好  收不到提示
        boolean b = job.waitForCompletion(true);//任务不管成功失败都等待结束后退出  并打印日志信息
        System.exit(b ? 0 : 1);
    }
}

