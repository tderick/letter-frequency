package it.unipi.hadoop.frequencycalculator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class LetterFrequencyCalculator {

//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length < 2) {
//            System.err.println("Usage: hadoop jar letter-frequency-1.0-SNAPSHOT.jar it.unipi.hadoop.drstrange.LetterFrequencyAnalyzer <in> [<in>...] <out>");
//            System.exit(2);
//        }
//        Job job = Job.getInstance(conf, "Letter Frequency Analysis");
//        job.setJarByClass(LetterFrequencyCalculator.class);
//        job.setMapperClass(LetterFrequencyMapper.class);
//        job.setCombinerClass(LetterFrequencyReducer.class);
//        job.setReducerClass(LetterFrequencyReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(FloatWritable.class);
//        for (int i = 0; i < otherArgs.length - 1; ++i) {
//            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
//        }
//        FileOutputFormat.setOutputPath(job,
//                new Path(otherArgs[otherArgs.length - 1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }

    public static class LetterFrequencyMapper extends Mapper<Object, Text, Text, FloatWritable> {

        private Map<Character, Integer> letterCounts; // In-mapper combining

        private final Text outputKey = new Text();
        private final FloatWritable outputValue = new FloatWritable();

        @Override
        protected void setup(Mapper<Object, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
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
        private int sharedValue;

        @Override
        protected void setup(Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
            sharedValue = Integer.parseInt(context.getConfiguration().get("shared.value"));
        }

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
            }
            float percentage = (sum/sharedValue)*100;
            result.set(percentage);
            context.write(key, result);
        }
    }
}
