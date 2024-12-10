import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {

    private Text outputValue = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double idf = 0;
        HashMap<String, Double> tfMap = new HashMap<String, Double>();

        for (Text val : values) {
            String value = val.toString();
            if (value.startsWith("TF@")) {
                String[] termDoc = value.split("=");
                String docID = termDoc[0].substring(3); // Extract docID
                double tf = Double.parseDouble(termDoc[1]);
                tfMap.put(docID, tf);
            } else if (value.startsWith("IDF=")) {
                idf = Double.parseDouble(value.split("=")[1]);
            }
        }

        // Calculate TF-IDF for each document
        for (String docID : tfMap.keySet()) {
            double tfidf = tfMap.get(docID) * idf;
            outputValue.set(String.valueOf(tfidf));
            context.write(new Text(key.toString() + "@" + docID), outputValue);
        }
    }
}
