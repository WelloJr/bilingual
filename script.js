package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int df = 0;
        for (@SuppressWarnings("unused") Text ignored : values) {
            df++;
        }

        if (df == 0) {
            return; // Ignore terms with DF = 0
        }

        int totalDocs = 10;
        double idf = Math.log10((double) totalDocs / df);

        // Emit DF and IDF
        result.set(df + " | " + idf);
        context.write(key, result);
    }
}
