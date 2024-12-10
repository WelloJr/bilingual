package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int docCount = 0;

        for (Text value : values) {
            docCount++;  // Count the number of documents for each term
        }

        // Compute IDF: IDF = log10(N / DF), where N = 10 (total number of documents)
        double idf = Math.log10(10.0 / docCount);

        result.set(String.valueOf(idf));
        context.write(key, result);
    }
}
