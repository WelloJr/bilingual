import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IDFDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: IDFDriver <input path> <output path> <totalDocuments>");
            System.exit(-1);
        }

        // Create a Configuration object
        Configuration conf = new Configuration();

        // Set the total number of documents as a configuration parameter
        conf.set("totalDocuments", args[2]);

        // Initialize the Job object with the Configuration and job name
        Job job = new Job(conf);
        job.setJobName("IDF Calculation"); // Explicitly set the job name

        // Specify the jar class
        job.setJarByClass(IDFDriver.class);

        // Set the Mapper and Reducer classes
        job.setMapperClass(IDFMapper.class);
        job.setReducerClass(IDFReducer.class);

        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exit after job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
