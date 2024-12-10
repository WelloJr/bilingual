import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Input: <term, [TF@docID=TF, IDF=value]>
        double idf = 0.0;
        HashMap<String, Double> tfMap = new HashMap<>();

        // Parse TF and IDF values
        for (Text value : values) {
            String val = value.toString();
            if (val.startsWith("TF@")) {
                String[] parts = val.split("=");
                String docID = parts[0].substring(3); // Extract docID
                double tf = Double.parseDouble(parts[1]); // Extract TF
                tfMap.put(docID, tf);
            } else if (val.startsWith("IDF=")) {
                idf = Double.parseDouble(val.split("=")[1]); // Extract IDF
            }
        }

        // Calculate TF-IDF and emit results
        for (String docID : tfMap.keySet()) {
            double tfidf = tfMap.get(docID) * idf;
            outputKey.set(docID);
            outputValue.set(key.toString() + ":" + tfidf); // <docID, term:TFIDF>
            context.write(outputKey, outputValue);
        }
    }
}
