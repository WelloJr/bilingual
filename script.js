package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double idf = 0.0;
        Map<String, Integer> tfMap = new HashMap<>();

        // Separate TF and IDF data
        for (Text val : values) {
            String value = val.toString();
            if (value.contains("|")) {
                // Extract IDF value
                String[] parts = value.split("\\|");
                idf = Double.parseDouble(parts[1].trim());
            } else {
                // Extract TF data
                String[] docData = value.split(";");
                for (String doc : docData) {
                    String[] docParts = doc.split(":");
                    String docId = docParts[0].trim();
                    int tf = Integer.parseInt(docParts[1].trim());
                    tfMap.put(docId, tf);
                }
            }
        }

        // Calculate TF-IDF for each document
        StringBuilder tfidfOutput = new StringBuilder();
        for (int docId = 1; docId <= 10; docId++) {
            String docKey = "doc" + docId;
            int tf = tfMap.containsKey(docKey) ? tfMap.get(docKey) : 0;
            double tfidf = tf * idf; // TF-IDF = TF * IDF
            tfidfOutput.append(docKey).append(":").append(tfidf).append("; ");
        }

        // Write the TF-IDF output to context
        result.set(tfidfOutput.toString().trim());
        context.write(key, result);
    }
}
