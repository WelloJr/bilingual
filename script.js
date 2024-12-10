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

        Configuration conf = new Configuration();
        conf.set("totalDocuments", args[2]); // Pass total document count as a configuration parameter

        Job job = new Job(conf, "IDF Calculation"); // Java 7-compatible Job instantiation
        job.setJarByClass(IDFDriver.class);
        job.setMapperClass(IDFMapper.class);
        job.setReducerClass(IDFReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
