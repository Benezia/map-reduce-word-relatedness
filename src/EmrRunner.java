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
	private static final String PLACEMENT_TYPE = "us-west-2a";
	private static final String HADOOP_VER = "2.7.2";
	private static final String ACTION_ON_FAIL = "TERMINATE_JOB_FLOW";
	private static final String CREDS_FILE = "./AwsCredentials.properties";
	private static final String ENDPOINT = "elasticmapreduce.us-west-2.amazonaws.com";
	private static final String INSTANCE_TYPE = InstanceType.M1Large.toString();
	
	public static void runEmrJob(int k) throws IllegalArgumentException, IOException {
		File creds = new File(CREDS_FILE);
		
		AmazonElasticMapReduce mapReduce;
		
		if (creds.exists()) 
			mapReduce = new AmazonElasticMapReduceClient(new PropertiesCredentials(creds));
		else
			throw new FileNotFoundException("Could not find credentials file");
		
		mapReduce.setEndpoint(ENDPOINT);
		 
		HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
		    .withJar("s3n://dsps161-ass2-binaries/WordCount.jar") // This should be a full map reduce application.
		    .withMainClass("WordCount")
		    .withArgs("s3n://dsp112/eng.corp.10k" /*input*/, 
		    		"s3n://dsps161-ass2-output/output" /*output*/, 
		    		String.valueOf(k));
		 
		StepConfig stepConfig = new StepConfig()
		    .withName("test step")
		    .withHadoopJarStep(hadoopJarStep)
		    .withActionOnFailure(ACTION_ON_FAIL);
		 
		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
		    .withInstanceCount(2)
		    .withMasterInstanceType(INSTANCE_TYPE) 
		    .withSlaveInstanceType(INSTANCE_TYPE)
		    .withHadoopVersion(HADOOP_VER)
		    .withEc2KeyName("DSPS_Ass2_key")
		    .withKeepJobFlowAliveWhenNoSteps(false)
		    .withPlacement(new PlacementType(PLACEMENT_TYPE));
		 
		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
		    .withName("test job")
		    .withInstances(instances)
		    .withSteps(stepConfig)
		    .withJobFlowRole("EMR_EC2_DefaultRole")
		    .withServiceRole("EMR_DefaultRole")
		    .withReleaseLabel("emr-4.6.0")
		    .withLogUri("s3n://dsps161-ass2-logs/");
		 
		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		String jobFlowId = runJobFlowResult.getJobFlowId();
		System.out.println("Ran job flow with id: " + jobFlowId);
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			throw new IllegalArgumentException("Arg: k: missing.");
		}
		
		runEmrJob(Integer.valueOf(args[0]));
		
		System.out.println("Finished.");
	}

}
