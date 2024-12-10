TF Mapper
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFMapper extends Mapper<Object, Text, Text, Text> {
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Input: < term docID: freq ; docID: freq ; ... >
        String line = value.toString();
        String[] parts = line.split("<");
        if (parts.length == 2) {
            String term = parts[0].trim();
            String postings = parts[1].trim().replaceAll("[<>]", ""); // Remove < and >
            String[] docs = postings.split(";");

            for (String doc : docs) {
                String[] docFreq = doc.trim().split(":");
                if (docFreq.length == 2) {
                    String docID = docFreq[0].trim();
                    String freq = docFreq[1].trim();
                    word.set(term);
                    context.write(word, new Text(docID + ":" + freq));
                }
            }
        }
    }
}
