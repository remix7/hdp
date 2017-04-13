package cn.echoes.hadoop.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * -------------------------------------
 * 统计流量  根据总流量倒序排序
 * -------------------------------------
 * Created by liutao on 2017/4/13 23:09.
 */
public class FlowCountSort {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("mapreduce.framework.name","yarn");  告诉mapreduce  yarn在哪 并且以yarn执行
//        conf.set("yarn.resourcemanage.hostname","hostname");
        Job job = Job.getInstance(conf);
        //设置jar包的路径
        job.setJarByClass(FlowCountSort.class);
        //设置任务mapper reducer 类
        job.setMapperClass(FlowCountSort.FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSort.FlowCountSortReduce.class);
        //设置map输出类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        //设置reduce 输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //设置输入目录
        FileInputFormat.setInputPaths(job, new Path("/flow/input"));
        //设置输出目录
        FileOutputFormat.setOutputPath(job, new Path("/flow/output"));
        //将任务提交给yarn
//        job.submit();  这种方式不好  收不到提示
        boolean b = job.waitForCompletion(true);//任务不管成功失败都等待结束后退出  并打印日志信息
        System.exit(b ? 0 : 1);
    }

    static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
        FlowBean bean = new FlowBean();
        Text text = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fileds = line.split("\t");
            String phone = fileds[0];
            long uFlow = Long.parseLong(fileds[1]);
            long dFlow = Long.parseLong(fileds[2]);
            bean.set(uFlow, dFlow);
            text.set(phone);
            context.write(bean, text);
        }
    }

    static class FlowCountSortReduce extends Reducer<FlowBean, Text, Text, FlowBean> {
        @Override
        protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(values.iterator().next(), key);
        }
    }
}
