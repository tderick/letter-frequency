package it.unipi.hadoop;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TotalCharacterCount {

    public static class TotalCountMapper extends Mapper<Object, Text, Text, LongWritable> {

        private final Text outputKey = new Text("total");
        private final LongWritable outputValue = new LongWritable();
        private Map<Character, Long> totalCount; // In-mapper combining

        @Override
        protected void setup(Mapper<Object, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            totalCount = new HashMap<>();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            char[] tokenizer = StringUtils.deleteWhitespace(line).toCharArray();

            for (char c : tokenizer) {
                if (((int) c >= 97 && (int) c <= 122)) {
                    totalCount.merge('T', 1L, Long::sum);
                }
            }
        }

        @Override
        protected void cleanup(Mapper<Object, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            for (Map.Entry<Character, Long> entry : totalCount.entrySet()) {
                outputValue.set(entry.getValue());
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class TotalCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0L;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }
}
