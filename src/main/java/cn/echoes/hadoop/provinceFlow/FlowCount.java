package cn.echoes.hadoop.provinceFlow;

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
 * 统计号码的上行和下行流量
 * 自定义过滤条件
 * 分省统计
 * partitioner 为统计条件
 * 设置reduce个数来 实现分省统计
 * -------------------------------------
 * Created by liutao on 2017/4/9 23:40.
 */
public class FlowCount {
    /**
     * mapper
     */
    static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fileds = line.split("\t");
            String phone = fileds[1];
            long uFlow = Long.parseLong(fileds[fileds.length - 3]);
            long dFlow = Long.parseLong(fileds[fileds.length - 2]);
            context.write(new Text(phone), new FlowBean(uFlow, dFlow));

        }
    }

    /**
     * reduce
     */
    static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long uFlowCount = 0;
            long dFlowCount = 0;
            for (FlowBean fb : values) {
                uFlowCount += fb.getuFlow();
                dFlowCount += fb.getdFlow();
            }
            context.write(key, new FlowBean(uFlowCount, dFlowCount));
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("mapreduce.framework.name","yarn");  告诉mapreduce  yarn在哪 并且以yarn执行
//        conf.set("yarn.resourcemanage.hostname","hostname");
        Job job = Job.getInstance(conf);
        //设置jar包的路径
        job.setJarByClass(FlowCount.class);
        //设置任务mapper reducer 类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);
        //设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //设置自定义partitioner
        job.setPartitionerClass(ProvincePartitioner.class);
        // 设置reduce的个数
        job.setNumReduceTasks(5);
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
}
