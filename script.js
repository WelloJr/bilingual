package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> docFrequencyMap = new HashMap<>();
        int df = 0;

        // Populate map with actual term frequencies from the values
        for (Text val : values) {
            String[] parts = val.toString().split(":");
            String docId = parts[0].trim();
            int count = Integer.parseInt(parts[1].trim());
            docFrequencyMap.put(docId, count);

            if (count > 0) {
                df++;
            }
        }

        // StringBuilder to hold the final TF output
        StringBuilder output = new StringBuilder();
        for (int docId = 1; docId <= 10; docId++) {
            String docKey = "doc" + docId;
            int freq = docFrequencyMap.containsKey(docKey) ? docFrequencyMap.get(docKey) : 0;
            output.append(docKey).append(":").append(freq).append("; ");
        }

        // Emit the DF as part of the value but don't display it
        result.set(output.toString().trim() + "| DF:" + df);
        context.write(key, result);
    }
}
