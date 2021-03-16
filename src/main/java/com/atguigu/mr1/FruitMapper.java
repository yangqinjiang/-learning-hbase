package com.atguigu.mr1;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class FruitMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
