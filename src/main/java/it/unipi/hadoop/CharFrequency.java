package it.unipi.hadoop;

import it.unipi.hadoop.frequencycalculator.LetterFrequencyCalculator;
import it.unipi.hadoop.totalcharcounter.TotalCharacterCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

public class CharFrequency {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: CharFrequency <input> <intermediate-output> <final-output>");
            System.exit(2);
        }

//        String tempDir = "/tmp/hadoop-job-temp";
//        conf.set("temp.dir", tempDir);

        // First job configuration
        Job firstJob = Job.getInstance(conf, "Total Character Count");
        firstJob.setJarByClass(TotalCharacterCount.class);
        firstJob.setMapperClass(TotalCharacterCount.TotalCountMapper.class);
        firstJob.setReducerClass(TotalCharacterCount.TotalCountReducer.class);
        firstJob.setOutputKeyClass(Text.class);
        firstJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(firstJob, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(firstJob, new Path(otherArgs[1]));

        if (!firstJob.waitForCompletion(true)) {
            System.exit(1);
        }

        // Read the output from the temporary file
        FileSystem fs = FileSystem.get(new URI(otherArgs[1]+"/part-r-00000"), conf);
        Path tempFilePath = new Path(otherArgs[1]+"/part-r-00000");
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(tempFilePath)));
        String line;
        while ((line = reader.readLine()) != null) {
            // Assume the file contains a single value
            String[] words = line.split("\t");
            if( words.length == 2 && words[0].equals("total")) {
                conf.set("shared.value", words[1]);
            }
        }
        reader.close();

    /// Second job configuration
        Job secondJob = Job.getInstance(conf, "Second Job");
        secondJob.setJarByClass(LetterFrequencyCalculator.class);
        secondJob.setMapperClass(LetterFrequencyCalculator.LetterFrequencyMapper.class);
        secondJob.setReducerClass(LetterFrequencyCalculator.LetterFrequencyReducer.class);
        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(secondJob, new Path(otherArgs[0])); // Intermediate output from the first job
        FileOutputFormat.setOutputPath(secondJob, new Path(otherArgs[2]));

        System.exit(secondJob.waitForCompletion(true) ? 0 : 1);
    }
}