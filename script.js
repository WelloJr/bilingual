import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TFIDFDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: TFIDFDriver <TF input path> <IDF input path> <output path>");
            System.exit(-1);
        }

        // Create a Configuration object
        Configuration conf = new Configuration();

        // Initialize the job
        Job job = new Job(conf, "TF-IDF Calculation");

        // Set the jar class
        job.setJarByClass(TFIDFDriver.class);

        // Set the Mapper and Reducer classes
        job.setMapperClass(TFIDFMapper.class);
        job.setReducerClass(TFIDFReducer.class);

        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the input and output paths for TF and IDF jobs
        FileInputFormat.addInputPath(job, new Path(args[0])); // TF input path
        FileInputFormat.addInputPath(job, new Path(args[1])); // IDF input path
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // TF-IDF output path

        // Exit after job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
