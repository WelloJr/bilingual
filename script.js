package reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;  // Make sure to import HashMap

public class PositionalIndexReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Map to store document IDs and positions
        Map<String, List<String>> positionalMap = new HashMap<>();

        // Process each value (docID:position)
        for (Text value : values) {
            String[] docAndPos = value.toString().split(":"); // Split into docID and position
            String docID = docAndPos[0];
            String position = docAndPos[1];

            // Add position to the respective docID list in the map
            if (!positionalMap.containsKey(docID)) {
                positionalMap.put(docID, new ArrayList<String>()); // Explicitly declare ArrayList<String>
            }
            positionalMap.get(docID).add(position);
        }

        // Prepare output in the required format
        StringBuilder formattedOutput = new StringBuilder("< ");
        formattedOutput.append(key.toString()); // Append the term (key)

        // Loop over the positional map to add docID and positions
        for (Map.Entry<String, List<String>> entry : positionalMap.entrySet()) {
            formattedOutput.append(" doc").append(entry.getKey()).append(": ");
            // Manually join the positions in the list
            List<String> positions = entry.getValue();
            for (int i = 0; i < positions.size(); i++) {
                formattedOutput.append(positions.get(i));
                if (i < positions.size() - 1) {
                    formattedOutput.append(", ");
                }
            }
            formattedOutput.append(" ; ");
        }
        formattedOutput.append(">");

        // Write output to context
        context.write(key, new Text(formattedOutput.toString()));
    }
}
