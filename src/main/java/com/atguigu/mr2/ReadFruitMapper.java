package com.atguigu.mr2;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ReadFruitMapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // 将fruit的name和color提取出来,相当于将每一行数据读取出来放入到put对象中
        Put put = new Put(key.get());
        //遍历添加column行
        for (Cell cell : value.rawCells()) {
            //添加/克隆列族
            if("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
                //添加/克隆 name
                if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    //将该列cell加入到put对象中
                    put.add(cell);
                }else if("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    //添加/克隆 color
                    //将该列cell加入到put对象中
                    //put.add(cell);
                }
            }
        }
        //将从fruit读取到的每行数据写入到context中,作为map的输出
        context.write(key,put);
    }
}
