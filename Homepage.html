package part2;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartTwoDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new PartTwoDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PartTwoDriver <input path> <output path>");
            return -1;
        }

        // Get the configuration object
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "TF-IDF Calculation");

        // Set the Jar class
        job.setJarByClass(PartTwoDriver.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(TFMapper.class);
        job.setReducerClass(TFReducer.class);

        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input and output paths from args
        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input directory from Part One output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Output directory for TF, IDF, TF-IDF

        // Cleanup if the output path exists
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[1]);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);  // delete output if it exists
        }

        // Wait for job completion
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
