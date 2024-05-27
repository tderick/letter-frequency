package it.unipi.hadoop;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

public class LetterFrequencyCalculator {

    public static class LetterFrequencyMapper extends Mapper<Object, Text, Text, FloatWritable> {

        private final Text outputKey = new Text();
        private final FloatWritable outputValue = new FloatWritable();
        private Map<Character, Integer> letterCounts; // In-mapper combining

        @Override
        protected void setup(Mapper<Object, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
            letterCounts = new HashMap<>();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            char[] tokenizer = StringUtils.deleteWhitespace(line).toCharArray();

            for (char c : tokenizer) {
                if (((int) c >= 97 && (int) c <= 122)) {
                    letterCounts.merge(c, 1, Integer::sum);
                }
            }
        }

        @Override
        protected void cleanup(Mapper<Object, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
            for (Map.Entry<Character, Integer> entry : letterCounts.entrySet()) {
                outputKey.set(Character.toString(entry.getKey()));
                outputValue.set(entry.getValue());
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class LetterFrequencyReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        private final FloatWritable result = new FloatWritable();
        private int totalCharacterCount;

        @Override
        protected void setup(Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
            totalCharacterCount = Integer.parseInt(context.getConfiguration().get("total.character.count"));
        }

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
            }
            float percentage = (sum / totalCharacterCount) * 100;
            DecimalFormat decfor = new DecimalFormat("0.00");
            float roundedPercentage = Float.parseFloat(decfor.format(percentage));
            result.set(roundedPercentage);
            context.write(key, result);
        }
    }

    public static class LetterFrequencyCombiner extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private final FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
