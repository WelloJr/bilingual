import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFIDFMapper extends Mapper<Object, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Input format: Either <term@docID, TF> or <term, IDF>
        String line = value.toString();
        String[] parts = line.split("\\t");

        if (parts[0].contains("@")) {
            // TF Input: <term@docID, TF>
            String[] termDoc = parts[0].split("@");
            String term = termDoc[0];
            String docID = termDoc[1];
            outputKey.set(term);
            outputValue.set("TF@" + docID + "=" + parts[1]); // Emit <term, TF@docID=TF>
        } else {
            // IDF Input: <term, IDF>
            String term = parts[0];
            String idf = parts[1];
            outputKey.set(term);
            outputValue.set("IDF=" + idf); // Emit <term, IDF=value>
        }
        context.write(outputKey, outputValue);
    }
}
