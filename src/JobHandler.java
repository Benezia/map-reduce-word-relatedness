
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

	

public class JobHandler extends Configured implements Tool {
	 private static final String OUTPUT_PATH = "intermediate_output";

	 
		public static void main (String args[]) throws Exception {
			if (args.length > 0) {
			    try {
			        Integer.parseInt(args[2]);
			    } catch (NumberFormatException e) {
			        System.err.println("Argument" + args[2] + " must be an integer.");
			        System.exit(1);
			    }
			} else {
				System.err.println("K argument is missing!");
				System.exit(1);
			}
			
			ToolRunner.run(new Configuration(), new JobHandler(), args);
		}
			
			
		 public int run(String[] args) throws Exception {
			Job job = Job1.activate(args[0], OUTPUT_PATH);
			job.waitForCompletion(true);
			job = Job2.activate(OUTPUT_PATH, args[1], args[2]);

			return job.waitForCompletion(true) ? 0 : 1;

		}
		
}
