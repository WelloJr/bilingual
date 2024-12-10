package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int docCount = 0;

        for (@SuppressWarnings("unused") Text ignored : values) {
            docCount++;
        }

        double idf = Math.log10(10.0 / docCount); // Assuming 10 documents
        result.set(String.valueOf(idf));
        context.write(key, result);
    }
}
