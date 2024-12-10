package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder output = new StringBuilder();
        double idf = 0.0;

        for (Text val : values) {
            String[] parts = val.toString().split(";");
            for (String part : parts) {
                String[] tfAndIdf = part.split(":");
                String docId = tfAndIdf[0];
                int tf = Integer.parseInt(tfAndIdf[1]);
                idf = Double.parseDouble(tfAndIdf[2]);
                double tfidf = tf * idf;

                output.append(docId).append(":").append(tfidf).append("; ");
            }
        }

        result.set(output.toString().trim());
        context.write(key, result);
    }
}
