package it.unipi.hadoop.frequencycalculator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PercentageJoinMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        if (parts.length == 2) {
            if (parts[0].equals("total")) {
                context.write(new Text("total"), new Text(parts[1]));
            } else {
                context.write(new Text(parts[0]), new Text("char_count\t" + parts[1]));
            }
        }
    }
}