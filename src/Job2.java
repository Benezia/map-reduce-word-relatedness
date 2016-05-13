import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Job2 {

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
	
	
	public static class Job2Reducer extends Reducer<WordPair,IntWritable,LimitedTreeSet,IntWritable> {
		private IntWritable result = new IntWritable();
		private int k = 0;
		private int c2 = 0;
		private double joint;
		private double dice;
		private double geometric;
		private LimitedTreeSet jointTree, diceTree, geometricTree;
		
		
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			k = context.getConfiguration().getInt("k", 10);
			jointTree = new LimitedTreeSet(k);
			diceTree = new LimitedTreeSet(k);
			geometricTree = new LimitedTreeSet(k);	
		}
		
		public void reduce(WordPair keyPair, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			
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
				
				//Calculate Probabilities:
				joint = (double)sum/(double)(keyPair.getN().get());
				dice = (double)(2*sum)/(double)(keyPair.getC1().get() + keyPair.getC1().get());
				geometric = Math.sqrt(joint*dice);
				
				//add to treeset:
				WordPair copy = new WordPair(keyPair);
				jointTree.add(new Node<WordPair>(joint, copy));
				diceTree.add(new Node<WordPair>(dice, copy));
				geometricTree.add(new Node<WordPair>(geometric, copy));
			}

		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(jointTree,new IntWritable(1));
			context.write(diceTree,new IntWritable(2));
			context.write(geometricTree, new IntWritable(3));
		}
	}
	

	
public static class Job2OutputFormat extends FileOutputFormat<LimitedTreeSet, IntWritable> {
  @Override
  public Job2RecordWriter getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
     Path path = FileOutputFormat.getOutputPath(arg0);
     Path fullPath = new Path(path, "result" + arg0.getTaskAttemptID().getTaskID().getId());
     FileSystem fs = path.getFileSystem(arg0.getConfiguration());
     FSDataOutputStream fileOut = fs.create(fullPath, arg0);
     return new Job2RecordWriter(fileOut);
  }
}

public static class Job2RecordWriter extends RecordWriter<LimitedTreeSet, IntWritable> {
	private boolean displayDecade = true;
    private DataOutputStream out;

    public Job2RecordWriter(DataOutputStream stream) {
        out = stream;
    }

    @Override
    public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
        out.close();
    }

    @Override
    public void write(LimitedTreeSet tree, IntWritable index) throws IOException, InterruptedException {
    	if (displayDecade) {
	    	WordPair pair = (WordPair)tree.first().value;
	    	int decade = pair.getDecade().get();
	    	out.writeBytes("Decade: "+ decade + "\n\n");
	    	displayDecade = false;
    	}
    	switch(index.get()) {
    	case 1:
	    	out.writeBytes("Joint Probability: \n");
    		break;
    	case 2:
	    	out.writeBytes("Dice Coefficient: \n");
    		break;
    	case 3:
	    	out.writeBytes("Geometric Mean: \n");
    		break;
		default:
	    	out.writeBytes("ERROR \n");
    	}
    	
    	@SuppressWarnings("rawtypes")
		Node node;
    	double stat;
    	WordPair pair;
    	int treesize = tree.size();
    	for (int i=1; i<=treesize; i++) {
    		node = tree.pollLast();
    		stat = node.statistic;
    		pair = (WordPair)node.value;
	    	out.writeBytes(i+". " + pair.toString() + ": \t" + stat + "\n");
    	}
    	out.writeBytes("\n");
    }
}



	public static Job activate(String input, String output, String k) throws Exception {

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
		
		job.setOutputKeyClass(LimitedTreeSet.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(Job2OutputFormat.class);
		
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		return job;
	}

	
}