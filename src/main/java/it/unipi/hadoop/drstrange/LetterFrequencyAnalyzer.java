package it.unipi.hadoop.drstrange;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LetterFrequencyAnalyzer {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar letter-frequency-1.0-SNAPSHOT.jar it.unipi.hadoop.drstrange.LetterFrequencyAnalyzer <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Letter Frequency Analysis");
        job.setJarByClass(LetterFrequencyAnalyzer.class);
        job.setMapperClass(LetterFrequencyAnalyzer.LetterFrequencyMapper.class);
        job.setCombinerClass(LetterFrequencyAnalyzer.LetterFrequencyReducer.class);
        job.setReducerClass(LetterFrequencyAnalyzer.LetterFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class LetterFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {

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

    public static class LetterFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
