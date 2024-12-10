import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TFDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TFDriver <input path> <output path>");
            System.exit(-1);
        }

        // Create a Configuration object
        Configuration conf = new Configuration();

        // Initialize the job
        Job job = new Job(conf, "Term Frequency");

        // Set the jar class
        job.setJarByClass(TFDriver.class);

        // Set the Mapper and Reducer classes
        job.setMapperClass(TFMapper.class);
        job.setReducerClass(TFReducer.class);

        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exit after job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
