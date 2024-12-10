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
        Map<String, Double> docTfIdfMap = new HashMap<>();
        double idf = 0.0;
        StringBuilder output = new StringBuilder();

        for (Text val : values) {
            String[] parts = val.toString().split("\\|");
            String docData = parts[0].trim();
            idf = Double.parseDouble(parts[1].trim());

            String[] docParts = docData.split(";");
            for (String doc : docParts) {
                String[] docDetails = doc.trim().split(":");
                String docId = docDetails[0].trim();
                int tf = Integer.parseInt(docDetails[1].trim());
                double tfIdf = (1 + Math.log10(tf)) * idf;
                docTfIdfMap.put(docId, tfIdf);
            }
        }

        for (Map.Entry<String, Double> entry : docTfIdfMap.entrySet()) {
            output.append(entry.getKey()).append(":").append(entry.getValue()).append("; ");
        }

        result.set(output.toString().trim());
        context.write(key, result);
    }
}
