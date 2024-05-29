package it.unipi.hadoop;

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
        if (otherArgs.length != 4) {
            System.err.println("Usage: hadoop jar <jar file> it.unipi.hadoop.CharFrequencyCharFrequency <input> <intermediate-output> <final-output> <number-reducer>");
            System.exit(2);
        }

        // First job configuration
        Job firstJob = Job.getInstance(conf, "Total Character Count");
        firstJob.setJarByClass(TotalCharacterCount.class);
        firstJob.setMapperClass(TotalCharacterCount.TotalCountMapper.class);
        firstJob.setCombinerClass(TotalCharacterCount.TotalCountReducer.class);
        firstJob.setReducerClass(TotalCharacterCount.TotalCountReducer.class);
        firstJob.setOutputKeyClass(Text.class);
        firstJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(firstJob, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(firstJob, new Path(otherArgs[1]));

        if (!firstJob.waitForCompletion(true)) {
            System.exit(1);
        }

        // Read the output from the temporary file
        String path = otherArgs[1]+"/part-r-00000";
        FileSystem fs = FileSystem.get(new URI(path), conf);
        Path tempFilePath = new Path(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(tempFilePath)));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] words = line.split("\t");
            if( words.length == 2 && words[0].equals("total")) {
                conf.set("total.character.count", words[1]);
            }
        }
        reader.close();

    /// Second job configuration
        Job secondJob = Job.getInstance(conf, "Compute Character Frequency");
        secondJob.setJarByClass(LetterFrequencyCalculator.class);
        secondJob.setMapperClass(LetterFrequencyCalculator.LetterFrequencyMapper.class);
        secondJob.setCombinerClass(LetterFrequencyCalculator.LetterFrequencyCombiner.class);
        secondJob.setReducerClass(LetterFrequencyCalculator.LetterFrequencyReducer.class);
        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(secondJob, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(secondJob, new Path(otherArgs[2]));

        secondJob.setNumReduceTasks(Integer.parseInt(otherArgs[3]));

        System.exit(secondJob.waitForCompletion(true) ? 0 : 1);
    }
}