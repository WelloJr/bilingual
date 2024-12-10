import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {

    private static final int TOTAL_DOCS = 10; // Update this based on the total number of documents

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int docCount = 0;
        for (Text value : values) {
            docCount++;
        }

        // Calculate IDF: log(TOTAL_DOCS / docCount)
        double idf = Math.log((double) TOTAL_DOCS / docCount);
        context.write(key, new Text(String.valueOf(idf)));
    }
}
