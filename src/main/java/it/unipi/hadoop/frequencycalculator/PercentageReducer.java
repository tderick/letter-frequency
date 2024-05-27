package it.unipi.hadoop.frequencycalculator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PercentageReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        Map<String, Integer> charCounts = new HashMap<>();

        for (Text val : values) {
            String[] parts = val.toString().split("\t");
            if (parts.length == 1) {
                total = Integer.parseInt(parts[0]);
            } else if (parts.length == 2) {
                charCounts.put(key.toString(), Integer.parseInt(parts[1]));
            }
        }

        for (Map.Entry<String, Integer> entry : charCounts.entrySet()) {
            double percentage = (entry.getValue() / (double) total) * 100;
            context.write(new Text(entry.getKey()), new Text(String.format("%.2f%%", percentage)));
        }
    }
}