import java.io.IOException;

import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job2 {
	private static FileHandler logFile;
	private static Logger logger;
	public static class Job2Mapper extends Mapper<WordPair, IntWritable, WordPair, IntWritable> {
		public void map(WordPair key, IntWritable value, Context context) throws IOException, InterruptedException {
			if (key.getIsSum().get()) {
				key.setW2("*");
				context.write(key, value);
			} else {
				String w1 = key.getW1().toString();
				key.setW1(key.getW2().toString());
				key.setW2(w1);
				context.write(key, value);
			}
		}
	}
	
	      
	public static class Job2Partitioner extends Partitioner<WordPair, IntWritable> {
		@Override
		public int getPartition(WordPair key, IntWritable value, int numPartitions) {
			return key.getDecade().get() % numPartitions;
		}
	}
	
	
	public static class Job2Reducer extends Reducer<WordPair,IntWritable,WordPair,IntWritable> {
		private IntWritable result = new IntWritable();
		private Text textKey = new Text();
		private int k = 0;
		private int c2 = 0;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			k = context.getConfiguration().getInt("k", 10);
		}
		
		public void reduce(WordPair keyPair, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
		
			logger.log(Level.FINE, keyPair.toString());
			
			
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			
			if (keyPair.getIsSum().get())					// <w2,*>
				c2 = sum;
			else {
				keyPair.setC2(c2);							// <w2,w1>
				String w1 = keyPair.getW1().toString();
				keyPair.setW1(keyPair.getW2().toString());
				keyPair.setW2(w1);	
				context.write(keyPair, result);
			}

		}
	}


	public static Job activate(String input, String output, String k) throws Exception {
		Job2.initLogFile();

		Configuration conf = new Configuration();
	    conf.set("k", k);

		Job job = Job.getInstance(conf, "Job2");

		// For binary input
		
		job.setJarByClass(Job2.class);
		
		job.setMapperClass(Job2Mapper.class);
		job.setMapOutputKeyClass(WordPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(11);
		job.setPartitionerClass(Job2Partitioner.class);

		
		job.setReducerClass(Job2Reducer.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		return job;
	}

	
	  public static void initLogFile() {
	  		 try {
	  			 logFile = new FileHandler("logger.log", false);
	  		 	} catch (SecurityException | IOException e) {
	  			 e.printStackTrace();
	  		 }
	  		 logger = Logger.getLogger("");
	  		 logFile.setFormatter(new SimpleFormatter());
	  		 logger.addHandler(logFile);
	  		 logger.setLevel(Level.FINER);
	  		 }
	  
	
}