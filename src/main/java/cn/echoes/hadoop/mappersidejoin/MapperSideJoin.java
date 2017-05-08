package cn.echoes.hadoop.mappersidejoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * -------------------------------------
 * TODO
 * -------------------------------------
 * Created by liutao on 2017/5/8 22:59.
 */
public class MapperSideJoin {

    static class MapperSideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Map<String, String> pdInfoMap = new HashMap<>();
        Text k = new Text();

        /**
         * 通过阅读分类Mapper的源码发现：
         * setup方法是maptask处理数据之前调用一次
         * 可以用来做初始化工作
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("")));
            String line;
            while (StringUtils.isNotEmpty(line = br.readLine())) {
                pdInfoMap.put(line.split(",")[0], line.split(",")[1]);
            }
            br.close();
        }

        /**
         * 由于在map中就有完整的信息表  所以在map中就能实现join逻辑了
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fileds = value.toString().split("\t");
            String pdName = pdInfoMap.get(fileds[1]);
            k.set(value.toString() + "\t" + pdName);
            context.write(k, NullWritable.get());
        }
    }


    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(MapperSideJoin.class);
        job.setMapperClass(MapperSideJoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(""));
        FileOutputFormat.setOutputPath(job, new Path(""));

        //指定一个缓存文件到maptask工作目录
        /*job.addArchiveToClassPath();*/  // 缓存jar到task运行节点的classpath中
        /*job.addCacheArchive()*/  //缓存压缩包到task运行节点的工作目录
        /*job.addCacheFile();*/    //缓存普通文件到task运行节点的工作目录
        /*job.addFileToClassPath();*/  //缓存普通文件到task运行的classpath中
        job.addCacheFile(new URI(""));

        job.setNumReduceTasks(0);

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
