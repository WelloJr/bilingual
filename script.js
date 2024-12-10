package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TFReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder output = new StringBuilder();

        for (Text val : values) {
            output.append(val.toString()).append("; ");
        }

        result.set(output.toString().trim());
        context.write(key, result);
    }
}
