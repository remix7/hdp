package cn.echoes.hadoop.reduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

/**
 * -------------------------------------
 * TODO
 * -------------------------------------
 * Created by liutao on 2017/2/26 20:45.
 */
public class TestReduce extends Reducer<Text, Text, Text, Text> {
    private Text docIds = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        HashSet<Text> uniqueDocIds = new HashSet<>();
        for (Text docId : values) {
            uniqueDocIds.add(docId);
        }
        docIds.set(new Text(StringUtils.join(uniqueDocIds, ",")));
        context.write(key, docIds);
    }
}
