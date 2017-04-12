package cn.echoes.hadoop.provinceFlow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * -------------------------------------
 * k2 map输出键类型 v2 map输出值类型
 * -------------------------------------
 * Created by liutao on 2017/4/12 23:48.
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    // 模拟外部字典
    private static Map<String, Integer> proviceDict = new HashMap<>();

    static {
        proviceDict.put("135", 0);
        proviceDict.put("136", 1);
        proviceDict.put("137", 2);
        proviceDict.put("138", 3);
    }

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        return proviceDict.get(text.toString().substring(0,3))==null?4:proviceDict.get(text.toString().substring(0,3));
    }
}
