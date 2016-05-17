import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

	public static class Job1Mapper extends Mapper<Object, Text, WordPair, LongWritable> {
		Set<String> stopWords = new HashSet<String>();
	    private WordPair wordPair = new WordPair();
	    LongWritable occurrences = new LongWritable();
	    private static final String IS_WORD_PATTERN = "^[a-zA-Z]{2,}$";
	    private static final String TAB = "\t";
	    private static final String SPACES = "\\s+";
	    String curr, mid;
	    
	    /*
        private static void combineStrings(String[] arr) {
            for (int i = 1; i<4; i++) {
                if (arr[i].equals("'") &&
                        arr[i+1].length() == 1 &&
                		arr[i+1].matches(IS_WORD_PATTERN) &&
                        !arr[i+1].equals("I")) {
                    System.out.println(arr[0] + " " + arr[1] + " " + arr[2] + " " + arr[3] + " " + arr[4]);
                    arr[i-1] += arr[i]+arr[i+1];
                    while (i<3) {
                        arr[i] = arr[i+2];
                        i++;   
                    }
                    arr[3] = "";
                    arr[4] = "";   
                    System.out.println(arr[0] + " " + arr[1] + " " + arr[2] + " " + arr[3] + " " + arr[4]);
                }
            }
           
        }*/

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
			String[] dataRow = value.toString().split(TAB);

			/*
			 split positions:
			 n-gram			(dataRow[0]) - The actual n-gram
			 year			(dataRow[1]) - The year for this aggregation
			 occurrences 	(dataRow[2]) - The number of times this n-gram appeared in this year
			 pages  		(dataRow[3]) - The number of pages this n-gram appeared on in this year
			 books 			(dataRow[4]) - The number of books this n-gram appeared in during this year
			*/
			if (dataRow.length != 5)
				return;
			
			occurrences.set(Integer.parseInt(dataRow[2]));
			int year = Integer.parseInt(dataRow[1]);
			String[] ngrams = dataRow[0].split(SPACES);

			if (ngrams.length != 5 || year < 1900) 
				return;
			
			//combineStrings(ngrams);
			
			String mid = ngrams[2].toLowerCase();
			
			// clean stop words & non-words
			if(stopWords.contains(mid) || !mid.matches(IS_WORD_PATTERN))
				return;
			
			wordPair.setDecade(year);
				
			for (int i : new int[]{0,1,3,4}) {
				mid = ngrams[2].toLowerCase();
				curr = ngrams[i].toLowerCase();	
				
				if(stopWords.contains(curr) || !curr.matches(IS_WORD_PATTERN))
					continue;
				
				// lexic ordering of pairs
				if (curr.compareTo(mid) < 0) {
					String temp = curr;
					curr = mid;
					mid = temp;
				}
				
				wordPair.setW1(mid);
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
	
	public static class Job1Combiner extends Reducer<WordPair,LongWritable,WordPair,LongWritable> {
		private LongWritable result = new LongWritable();
		public void reduce(WordPair keyPair, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
	          long sum = 0;
	          for (LongWritable val : values) 
	             sum += val.get();      
	          result.set(sum);
	          context.write(keyPair, result);
       }
	}

		
	
	      
	public static class Job1Partitioner extends Partitioner<WordPair, LongWritable> {
		@Override
		public int getPartition(WordPair key, LongWritable value, int numPartitions) {
			return key.getDecade().get() % numPartitions;
		}
	}
	
	
	public static class Job1Reducer extends Reducer<WordPair,LongWritable,WordPair,LongWritable> {
		private LongWritable result = new LongWritable();
		private long n = 0;
		private long c1 = 0;
		
		public void reduce(WordPair keyPair, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;

			for (LongWritable val : values) {
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

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Job1");

		// For binary input
		job.setInputFormatClass(SequenceFileInputFormat.class); 
		
		job.getConfiguration().set(STOP_WORDS_FILE, "stop_words.txt");
		
		job.setJarByClass(Job1.class);
		
		job.setMapperClass(Job1Mapper.class);
		job.setMapOutputKeyClass(WordPair.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setCombinerClass(Job1Combiner.class);
		job.setNumReduceTasks(11);
		job.setPartitionerClass(Job1Partitioner.class);
		job.setReducerClass(Job1Reducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(WordPair.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(input)); 
		FileOutputFormat.setOutputPath(job, new Path(output)); /*env_var: mapred_job_id */
		return job;
	}
	
	public static void main (String args[]) throws Exception {
		if (args.length != 2)
			throw new Exception("Incorrect args");
		
		activate(args[0], args[1]).waitForCompletion(true);
	}

}