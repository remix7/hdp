package cn.echoes.hadoop.sharedfriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

/**
 * -------------------------------------
 * TODO
 * 找出qq共同好友
 * A:B,C,D,E,F,G
 * B:A,E,H,C
 * -------------------------------------
 * Created by liutao on 2017/5/13 23:26.
 */
public class SharedFriendsStepTwo {
    static class SharedFriendsStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * A B,C,D  A是bcd的共同好友
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] friend_persons = line.split("\t");
            String friend = friend_persons[0];
            String[] persons = friend_persons[1].split(",");
            Arrays.sort(persons);
            for (int i = 0; i < persons.length - 2; i++) {
                for (int j = i + 1; j < persons.length - 1; j++) {
                    //这样相同的人-人 对<人-人，好友> 会到同一个reduce中
                    context.write(new Text(persons[i]+"-"+persons[j]),new Text(friend));
                }
            }
        }
    }

    static class SharedFriendsStepTwoReduce extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for(Text friend :values){
                sb.append(friend).append(" ");
            }
            context.write(key,new Text(sb.toString()));
        }
    }
}
