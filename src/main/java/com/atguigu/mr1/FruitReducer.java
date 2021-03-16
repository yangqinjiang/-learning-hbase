package com.atguigu.mr1;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

// 注意,这里继承hbase提供的TableReducer,方便插入数据到hbase数据库
public class FruitReducer extends TableReducer<LongWritable, Text, NullWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            //原始数据
            //    1001  apple   red
            //    1002  banlana   yellow
            String[] fields = value.toString().split("\t");

            //构建put, 写入到hbase数据库
            Put put = new Put(Bytes.toBytes(fields[0]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(fields[1]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(fields[2]));
            context.write(NullWritable.get(), put);
        }
    }
}
