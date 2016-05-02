import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class WordCount {
	final static String STOP_WORDS_FILE = "wc.stopwords.file";
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		Set<String> stopWords = new HashSet<String>();
		//private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
        @Override
        protected void setup(Context context) throws IOException{
            Configuration conf = context.getConfiguration();
            String stp_file_name = conf.get(STOP_WORDS_FILE);
            try {
	            InputStream res = WordCount.class.getResourceAsStream(stp_file_name);
	
	    	    BufferedReader fis = new BufferedReader(new InputStreamReader(res));

                String word;
                while((word = fis.readLine()) != null) {
                	//System.out.println("GOT: "+ word);
                    stopWords.add(word);
                }
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("error while reading stopwords",e);
            }
        }

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//System.out.println("GOT: "+ value.toString());
			String[] dataRow = value.toString().split("\t");
			// split positions:
			// n-gram - The actual n-gram
			// year - The year for this aggregation
			// occurrences - The number of times this n-gram appeared in this year
			// pages - The number of pages this n-gram appeared on in this year
			// books - The number of books this n-gram appeared in during this year
			IntWritable occurrences = new IntWritable(Integer.parseInt(dataRow[2]));
			int year = Integer.parseInt(dataRow[1]);
			String[] ngrams = dataRow[0].split("\\s+");
			
			
			if (ngrams.length != 5)
				return;
			
			String mid = ngrams[2].toLowerCase();

			if(stopWords.contains(mid))
				return;
			
			for (int i : new int[]{0,1,3,4}) {
				String curr = ngrams[i].toLowerCase();
				
				if (!stopWords.contains(curr)) {
					word.set(mid + "," + curr);
					context.write(word, occurrences);
				}
			}
			
		}
	}

	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");

		// For binary input
		job.setInputFormatClass(SequenceFileInputFormat.class); 
		
		job.getConfiguration().set(STOP_WORDS_FILE, "stop_words.txt");
		
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}