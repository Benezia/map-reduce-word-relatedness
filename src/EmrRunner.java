import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;



public class EmrRunner {
	private static final String LOG_LOCATION = "s3n://dsps161-ass2-logs/";
	private static final int NUM_OF_INSTANCES = 20;
	private static final String INTERMEDIATE_PATH = "hdfs:///intermediate/" /*output*/;
	private static final String S3_JAR = "s3n://dsps161-ass2-binaries/WordRelatedness.jar";
	private static final String PLACEMENT_TYPE = "us-east-1b";
	private static final String ENDPOINT = "elasticmapreduce.us-east-1.amazonaws.com";
	private static final String HADOOP_VER = "2.7.2";
	private static final String ACTION_ON_FAIL = "TERMINATE_JOB_FLOW";
	private static final String CREDS_FILE = "./AwsCredentials.properties";
	private static final String INSTANCE_TYPE = InstanceType.M1Large.toString();	
	//private static final String CORPUS = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-1M/5gram/data";
	private static final String CORPUS = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/5gram/data";
	//private static final String TEST_CORP = "s3n://dsp112/eng.corp.10k";
	
	public static void runEmrJob(int k) throws IllegalArgumentException, IOException {
		File creds = new File(CREDS_FILE);
		
		AmazonElasticMapReduce mapReduce;
		
		if (creds.exists()) 
			mapReduce = new AmazonElasticMapReduceClient(new PropertiesCredentials(creds));
		else
			throw new FileNotFoundException("Could not find credentials file");
		
		mapReduce.setEndpoint(ENDPOINT);
		 
		HadoopJarStepConfig JarStep1 = new HadoopJarStepConfig()
		    .withJar(S3_JAR) // This should be a full map reduce application.
		    .withMainClass("Job1")
		    .withArgs(CORPUS /*input*/, 
		    		INTERMEDIATE_PATH);
		
		HadoopJarStepConfig JarStep2 = new HadoopJarStepConfig()
		    .withJar(S3_JAR) // This should be a full map reduce application.
		    .withMainClass("Job2")
		    .withArgs(INTERMEDIATE_PATH, 
		    		"s3n://dsps161-ass2-output/output_eng_1m_run5" /*output*/, 
		    		String.valueOf(k));
		 
		StepConfig step1Config = new StepConfig()
		    .withName("Step1")
		    .withHadoopJarStep(JarStep1)
		    .withActionOnFailure(ACTION_ON_FAIL);
		
		StepConfig step2Config = new StepConfig()
		    .withName("Step2")
		    .withHadoopJarStep(JarStep2)
		    .withActionOnFailure(ACTION_ON_FAIL);
		 
		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
		    .withInstanceCount(NUM_OF_INSTANCES)
		    .withMasterInstanceType(INSTANCE_TYPE) 
		    .withSlaveInstanceType(INSTANCE_TYPE)
		    .withHadoopVersion(HADOOP_VER)
		    .withEc2KeyName("DSPS_Ass2_key")
		    .withKeepJobFlowAliveWhenNoSteps(false)
		    .withPlacement(new PlacementType(PLACEMENT_TYPE));
		 
		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
		    .withName("Word Relatedness")
		    .withInstances(instances)
		    .withSteps(step1Config, step2Config)
		    .withJobFlowRole("EMR_EC2_DefaultRole")
		    .withServiceRole("EMR_DefaultRole")
		    .withReleaseLabel("emr-4.6.0")
		    .withLogUri(LOG_LOCATION);
		 
		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		String jobFlowId = runJobFlowResult.getJobFlowId();
		System.out.println("Ran job flow with id: " + jobFlowId);
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			throw new IllegalArgumentException("Arg: k: missing.");
		}
		
		runEmrJob(Integer.valueOf(args[0]));
		
		System.out.println("Job Submitted. See EMR Console.");
	}

}
