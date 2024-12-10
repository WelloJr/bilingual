import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IDFMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Input: < term, doc1 freq1 doc2 freq2 ... >
        String line = value.toString();
        String[] parts = line.split("\\t");
        if (parts.length > 1) {
            String term = parts[0];
            for (int i = 1; i < parts.length; i++) {
                if (!parts[i].equals("0")) {
                    word.set(term);
                    context.write(word, one);
                }
            }
        }
    }
}
