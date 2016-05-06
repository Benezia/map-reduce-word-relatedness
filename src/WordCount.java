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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class WordCount {
	final static String STOP_WORDS_FILE = "wc.stopwords.file";
	private static FileHandler logFile;
	private static Logger logger;

	public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable> {
		Set<String> stopWords = new HashSet<String>();
		private MapWritable occurrenceMap = new MapWritable();
		private Text word = new Text();
		
        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String stp_file_name = conf.get(STOP_WORDS_FILE);
            try {
	            InputStream res = WordCount.class.getResourceAsStream(stp_file_name);
	
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
			
			logger.log(Level.FINE, value.toString());

			String mid = ngrams[2].toLowerCase();

			if(stopWords.contains(mid))
				return;

	          word.set(mid);
	          occurrenceMap.clear();

			for (int j : new int[]{0,1,3,4}) {
				String curr = ngrams[j].toLowerCase();
                Text neighbor = new Text(curr);
                if (!stopWords.contains(curr)) {
	                if(occurrenceMap.containsKey(neighbor)) {
	                   IntWritable count = (IntWritable)occurrenceMap.get(neighbor);
	                   count.set(count.get()+occurrences.get());
	                } else 
	                   occurrenceMap.put(neighbor,occurrences);
                }
	        }
			context.write(word,occurrenceMap);
	     }
   }


	

public static class StripesReducer extends Reducer<Text, MapWritable, Text, IntWritable> {
    private MapWritable incrementingMap = new MapWritable();

    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
    	Text word = new Text();
        incrementingMap.clear();
        
        for (MapWritable mapWritable : values) {
        	Set<Writable> neighbors = mapWritable.keySet();
        	
            for (Writable neighbor : neighbors) {  
                IntWritable fromCount = (IntWritable) mapWritable.get(neighbor);
                if (incrementingMap.containsKey(neighbor)) {
                    IntWritable count = (IntWritable) incrementingMap.get(neighbor);
                    count.set(count.get() + fromCount.get());
                } else {
                    incrementingMap.put(neighbor, fromCount);
                	word.set(key + "," + neighbor);
                    context.write(word, fromCount);
                }
            }
        }
        
    }
}



/*
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
*/






	public static void main(String[] args) throws Exception {
		WordCount.initLogFile();

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");

		// For binary input
		job.setInputFormatClass(SequenceFileInputFormat.class); 
		
		job.getConfiguration().set(STOP_WORDS_FILE, "stop_words.txt");
		
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		
		//job.setCombinerClass(StripesReducer.class);
		job.setReducerClass(StripesReducer.class);

		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
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