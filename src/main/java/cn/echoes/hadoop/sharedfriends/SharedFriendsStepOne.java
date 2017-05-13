package cn.echoes.hadoop.sharedfriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * -------------------------------------
 * TODO
 * 找出qq共同好友
 * A:B,C,D,E,F,G
 * B:A,E,H,C
 * -------------------------------------
 * Created by liutao on 2017/5/13 23:26.
 */
public class SharedFriendsStepOne {
    static class SharedFriendsStepOneMapper extends Mapper<LongWritable,Text,Text,Text>{
        //A:B,C,D,E,F
        //B:A,E,G,H,K
        //C:A,B,G,H,I
        /**
         * map:
         * B A ,  C A, D A ,E A , F A
         * A B, E B ,G B ,H B, K B              ==> B A,C   A B,C
         * A C , B C , G C , H C , I C
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] person_friends = line.split(":");
            String person = person_friends[0];
            String[] friends = person_friends[1].split(",");
            for(String friend : friends){
                context.write(new Text(friend),new Text(person));
            }
        }
    }

    static class SharedFriendsStepOneReduce extends Reducer<Text,Text,Text,Text>{

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for(Text person :values){
                sb.append(person).append(",");
            }
            context.write(key,new Text(sb.toString()));
        }
    }
}
