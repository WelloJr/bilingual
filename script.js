import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IDFReducer extends Reducer<Text, IntWritable, Text, Text> {
    private static final int TOTAL_DOCS = 10;

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int docCount = 0;
        for (IntWritable val : values) {
            docCount += val.get();
        }

        double idf = Math.log((double) TOTAL_DOCS / docCount);
        context.write(key, new Text(String.valueOf(idf)));
    }
}
