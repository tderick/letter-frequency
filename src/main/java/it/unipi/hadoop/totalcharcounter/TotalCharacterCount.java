package it.unipi.hadoop.totalcharcounter;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

public class TotalCharacterCount {

//    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//        Configuration conf = new Configuration();
//        String tempDir = "/tmp/hadoop-job-temp";
//        conf.set("temp.dir", tempDir);
//
//        Job job = Job.getInstance(conf, "Total Character Count");
//        job.setJarByClass(TotalCharacterCount.class);
//        job.setMapperClass(TotalCountMapper.class);
//        job.setReducerClass(TotalCountReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//        if (!job.waitForCompletion(true)) {
//            System.exit(1);
//        }
//    }

    public static class TotalCountMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Map<Character, Integer> totalCount; // In-mapper combining

        private final Text outputKey = new Text("total");
        private final IntWritable outputValue = new IntWritable();

        @Override
        protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            totalCount = new HashMap<>();
        }

        public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            char[] tokenizer = StringUtils.deleteWhitespace(line).toCharArray();

            for (char c : tokenizer) {
                if (((int) c >= 97 && (int) c <= 122)) {
                    totalCount.merge('T', 1, Integer::sum);
                }
            }
        }

        @Override
        protected void cleanup(Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            for (Map.Entry<Character, Integer> entry : totalCount.entrySet()) {
                outputValue.set(entry.getValue());
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class TotalCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));

//        Store Total Count
//            FileSystem fs = FileSystem.get(context.getConfiguration());
//            Path tempFilePath = new Path(context.getConfiguration().get("temp.dir"), "temp-output");
//            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(tempFilePath, true)));
//            writer.write(String.valueOf(sum));
//            writer.newLine();
//            writer.close();
        }
    }
}
