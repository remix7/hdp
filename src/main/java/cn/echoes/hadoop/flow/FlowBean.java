package cn.echoes.hadoop.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * -------------------------------------
 * 将上行和下行流量封装成一个bean
 * -------------------------------------
 * Created by liutao on 2017/4/9 23:42.
 */
public class FlowBean implements Writable {

    private long uFlow;
    private long dFlow;
    private long sumFlow;

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public long getuFlow() {
        return uFlow;
    }

    public void setuFlow(long uFlow) {
        this.uFlow = uFlow;
    }

    public long getdFlow() {
        return dFlow;
    }

    public void setdFlow(long dFlow) {
        this.dFlow = dFlow;
    }

    public FlowBean(long uFlow, long dFlow) {
        this.uFlow = uFlow;
        this.dFlow = dFlow;
        this.sumFlow = uFlow + dFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(uFlow);
        dataOutput.writeLong(dFlow);
        dataOutput.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.uFlow = dataInput.readLong();
        this.dFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return uFlow + "\t" + dFlow + "\t" + sumFlow;
    }

    public FlowBean() {
    }
}
