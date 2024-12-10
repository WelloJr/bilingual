import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text word = new Text();
    private IntWritable count = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Input format: <term doc1:position1, position2 ... ; doc2:position1, ... ; >
        String line = value.toString();
        String[] parts = line.split("<");
        
        if (parts.length == 2) {
            String term = parts[0].trim();
            String docs = parts[1].trim().replaceAll("[<>]", ""); // Remove < and >
            String[] docList = docs.split(";");

            for (String doc : docList) {
                String[] docTerm = doc.trim().split(":");
                String docID = docTerm[0].trim();
                int frequency = Integer.parseInt(docTerm[1].trim());
                
                word.set(term + "@" + docID);
                context.write(word, count);
            }
        }
    }
}
