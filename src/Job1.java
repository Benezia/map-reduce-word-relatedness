import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.HashSet;
import java.util.Set;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class Job1 {
	final static String STOP_WORDS_FILE = "wc.stopwords.file";
	private static FileHandler logFile;
	private static Logger logger;

    
	public static class Job1Mapper extends Mapper<Object, Text, WordPair, IntWritable> {
		Set<String> stopWords = new HashSet<String>();
	    private WordPair wordPair = new WordPair();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String stp_file_name = conf.get(STOP_WORDS_FILE);
            try {
	            InputStream res = Job1.class.getResourceAsStream(stp_file_name);
	
	    	    BufferedReader fis = new BufferedReader(new InputStreamReader(res));

                String word;
                while((word = fis.readLine()) != null) {
                    stopWords.add(word);
                }
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("error while reading stopwords",e);
            }
        }

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] dataRow = value.toString().split("\t");

			/*
			 split positions:
			 n-gram			(dataRow[0]) - The actual n-gram
			 year			(dataRow[1]) - The year for this aggregation
			 occurrences 	(dataRow[2]) - The number of times this n-gram appeared in this year
			 pages  		(dataRow[3]) - The number of pages this n-gram appeared on in this year
			 books 			(dataRow[4]) - The number of books this n-gram appeared in during this year
			*/
			IntWritable occurrences = new IntWritable(Integer.parseInt(dataRow[2]));
			int year = Integer.parseInt(dataRow[1]);
			String[] ngrams = dataRow[0].split("\\s+");
			
			if (ngrams.length != 5 || year < 1900) 
				return;
			
			//logger.log(Level.FINE, value.toString());

			String mid = ngrams[2].toLowerCase();

			if(stopWords.contains(mid))
				return;
			wordPair.setW1(mid);
			wordPair.setDecade(year);
				
				for (int i : new int[]{0,1,3,4}) {
					String curr = ngrams[i].toLowerCase();	
					if (!stopWords.contains(curr)) {
						wordPair.setW2(curr);
						wordPair.setIsTotalSum(false);
						context.write(wordPair, occurrences); //add <w1,w2>
						
						wordPair.setW2("*");
						wordPair.setIsSum(true);
						context.write(wordPair, occurrences); //add <w1,*>
						
						wordPair.setW1(curr);
						wordPair.setW2("**");
						context.write(wordPair, occurrences); //add <w2,**>	
						
						wordPair.setW1("*");
						wordPair.setW2("*");
						wordPair.setIsSum(false);
						wordPair.setIsTotalSum(true);
						context.write(wordPair, occurrences); //add <*,*>
					}
				}
				
				
			}
		}
	
	public static class Job1Combiner extends Reducer<WordPair,IntWritable,WordPair,IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(WordPair keyPair, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	          int sum = 0;
	          for (IntWritable val : values) 
	             sum += val.get();      
	          result.set(sum);
	          context.write(keyPair, result);
       }
	}

		
	
	      
	public static class Job1Partitioner extends Partitioner<WordPair, IntWritable> {
		@Override
		public int getPartition(WordPair key, IntWritable value, int numPartitions) {
			return key.getDecade().get() % numPartitions;
		}
	}
	
	
	public static class Job1Reducer extends Reducer<WordPair,IntWritable,WordPair,IntWritable> {
		private IntWritable result = new IntWritable();
		private int n = 0;
		private int c1 = 0;
		
		public void reduce(WordPair keyPair, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
		
			logger.log(Level.FINE, keyPair.toString());
			
			
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			
			if (keyPair.getIsTotalSum().get())  					// <*,*>
				n = sum;
			
			else if (keyPair.getIsSum().get()) {					// <w,*>
					if (keyPair.getW2().toString().equals("*"))		
						c1 = sum;									// <w1,*>
					else
						context.write(keyPair, result);				// <w2,*>
			}
			
			else  {													// <w1,w2>
					keyPair.setN(n);
					keyPair.setC1(c1);
					context.write(keyPair, result);
			}
		}
	}


	public static Job activate(String input, String output) throws Exception {
		Job1.initLogFile();

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Job1");

		// For binary input
		job.setInputFormatClass(SequenceFileInputFormat.class); 
		
		job.getConfiguration().set(STOP_WORDS_FILE, "stop_words.txt");
		
		job.setJarByClass(Job1.class);
		
		job.setMapperClass(Job1Mapper.class);
		job.setMapOutputKeyClass(WordPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setCombinerClass(Job1Combiner.class);
		
		job.setNumReduceTasks(11);
		job.setPartitionerClass(Job1Partitioner.class);

		
		job.setReducerClass(Job1Reducer.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(WordPair.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input)); 
		FileOutputFormat.setOutputPath(job, new Path(output)); /*env_var: mapred_job_id */
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