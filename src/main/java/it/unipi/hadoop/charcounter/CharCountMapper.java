package it.unipi.hadoop.charcounter;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CharCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Map<Character, Integer> letterCounts; // In-mapper combining

    private final Text outputKey = new Text();
    private final IntWritable outputValue = new IntWritable();

    @Override
    protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        letterCounts = new HashMap<>();
    }

    public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {

        String line = value.toString().toLowerCase();
        char[] tokenizer = StringUtils.deleteWhitespace(line).toCharArray();

        for (char c : tokenizer) {
            if (((int) c >= 97 && (int) c <= 122)) {
                letterCounts.merge(c, 1, Integer::sum);
            }
        }
    }

    @Override
    protected void cleanup(Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        for (Map.Entry<Character, Integer> entry : letterCounts.entrySet()) {
            outputKey.set(Character.toString(entry.getKey()));
            outputValue.set(entry.getValue());
            context.write(outputKey, outputValue);
        }
    }
}
