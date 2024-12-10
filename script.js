import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Input: <term, [docID:freq, docID:freq, ...]>
        Map<String, String> docFrequency = new HashMap<String, String>();

        for (Text val : values) {
            String[] docFreq = val.toString().split(":");
            String docID = docFreq[0];
            String freq = docFreq[1];
            docFrequency.put(docID, freq);
        }

        // Output in matrix form
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= 10; i++) { // Assuming 10 documents
            sb.append(docFrequency.containsKey("doc" + i) ? docFrequency.get("doc" + i) : "0").append("\t");
        }
        context.write(key, new Text(sb.toString().trim()));
    }
}
