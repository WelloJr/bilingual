import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFIDFMapper extends Mapper<Object, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Input: <term@docID, TF> from TF
        // Input: <term, IDF> from IDF
        String line = value.toString();
        String[] parts = line.split("\\t");

        if (parts.length == 2) {
            String term = parts[0];
            String data = parts[1];

            if (data.contains("=")) {
                // TF Input: <term@docID, TF>
                String[] termDoc = term.split("@");
                outputKey.set(termDoc[0]);
                outputValue.set("TF@" + termDoc[1] + "=" + data);
            } else {
                // IDF Input: <term, IDF>
                outputKey.set(term);
                outputValue.set("IDF=" + data);
            }
            context.write(outputKey, outputValue);
        }
    }
}
