package com.fgm.demo2.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @Auther: fgm
 * @Date: 2018/8/31 9:56
 */
public class SortPojo implements WritableComparable<SortPojo> {

    //组合key,第一部分是我们的第一列,第二部分是我们的第二列
    private String first;
    private int second;

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public SortPojo(String first, int second) {
        this.first = first;
        this.second = second;
    }
    /**
     * 方便设置字段
     */
    public void set(String first, int second) {
        this.first = first;
        this.second = second;
    }



    public SortPojo() { }

    /**
     * 重写比较器
     * @param o
     * @return
     */
    @Override
    public int compareTo(SortPojo o) {
        int comp = this.first.compareTo(o.first);
        if (comp!=0){
            return comp;
        }else {
            return Integer.valueOf(this.second).compareTo(Integer.valueOf(o.getSecond()));
        }
    }

    /**
     * 序列化
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(first);
        out.writeInt(second);
    }

    /**
     * 反序列化
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {

        this.first=in.readUTF();
        this.second=in.readInt();
    }

    @Override
    public String toString() {
        return "SortPojo{" +
                "first='" + first + '\'' +
                ", second=" + second +
                '}';
    }
}
