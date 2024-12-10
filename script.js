import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IDFMapper extends Mapper<Object, Text, Text, Text> {

    private Text term = new Text();
    private Text docID = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Input: <term doc1: freq ; doc2: freq ; ...>
        String line = value.toString();
        String[] parts = line.split("\\t");

        if (parts.length == 2) {
            String termValue = parts[0];
            String docDetails = parts[1];

            String[] docList = docDetails.split(";");
            for (String doc : docList) {
                term.set(termValue);
                docID.set(doc.split(":")[0].trim()); // Only emit the document ID
                context.write(term, docID);
            }
        }
    }
}
