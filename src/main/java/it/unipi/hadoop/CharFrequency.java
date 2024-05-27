package it.unipi.hadoop;

import it.unipi.hadoop.charcounter.CharCountMapper;
import it.unipi.hadoop.charcounter.CharCountReducer;
import it.unipi.hadoop.frequencycalculator.PercentageJoinMapper;
import it.unipi.hadoop.frequencycalculator.PercentageReducer;
import it.unipi.hadoop.totalcharcounter.TotalCountMapper;
import it.unipi.hadoop.totalcharcounter.TotalCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CharFrequency {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job 1: Character Count
        Job job1 = Job.getInstance(conf, "character count");
        job1.setJarByClass(CharFrequency.class);
        job1.setMapperClass(CharCountMapper.class);
        job1.setCombinerClass(CharCountReducer.class);
        job1.setReducerClass(CharCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        // Job 2: Total Character Count
        Job job2 = Job.getInstance(conf, "total count");
        job2.setJarByClass(CharFrequency.class);
        job2.setMapperClass(TotalCountMapper.class);
        job2.setCombinerClass(TotalCountReducer.class);
        job2.setReducerClass(TotalCountReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);

        // Job 3: Calculate Percentage
        Job job3 = Job.getInstance(conf, "calculate percentage");
        job3.setJarByClass(CharFrequency.class);
        job3.setMapperClass(PercentageJoinMapper.class);
        job3.setReducerClass(PercentageReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(args[1])); // character counts
        FileInputFormat.addInputPath(job3, new Path(args[2])); // total count
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}