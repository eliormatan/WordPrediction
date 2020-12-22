import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import software.amazon.awssdk.services.ec2.model.*;

public class Main {
        private static final AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        private static AmazonElasticMapReduce mapReduce;


    public static void main(String[] args) {
        mapReduce = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        //STEP1
        HadoopJarStepConfig divide = new HadoopJarStepConfig()
                .withJar("s3://dsp-211-ass2/divide.jar")
//                .withMainClass("divide.Mainclass") todo:find mainclass name
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/1gram/data","s3://dsp-211-ass2/divideOut");
        StepConfig stepDivide = new StepConfig()
                .withName("divide")
                .withHadoopJarStep(divide)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //STEP2
        HadoopJarStepConfig calcNT = new HadoopJarStepConfig()
                .withJar("s3://dsp-211-ass2/calcNT.jar")
//                .withMainClass("divide.Mainclass") todo:find mainclass name
                .withArgs("s3://dsp-211-ass2/divideOut","s3://dsp-211-ass2/calcNTOut");
        StepConfig stepCalcNT = new StepConfig()
                .withName("calcNT")
                .withHadoopJarStep(calcNT)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //STEP3
        HadoopJarStepConfig calcProb = new HadoopJarStepConfig()
                .withJar("s3://dsp-211-ass2/calcProb.jar")
//                .withMainClass("divide.Mainclass") todo:find mainclass name
                .withArgs("s3://dsp-211-ass2/calcNTOut","s3://dsp-211-ass2/calcProbOut");
        StepConfig stepCalcProb = new StepConfig()
                .withName("calcProb")
                .withHadoopJarStep(calcProb)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

		//STEP4
        HadoopJarStepConfig sortResults = new HadoopJarStepConfig()
                .withJar("s3://dsp-211-ass2/sortResults.jar")
//                .withMainClass("divide.Mainclass") todo:find mainclass name
                .withArgs("s3://dsp-211-ass2/calcProbOut","s3://dsp-211-ass2/sortResultsOut");
        StepConfig stepSortResults = new StepConfig()
                .withName("sortResults")
                .withHadoopJarStep(sortResults)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M4_LARGE.toString())
                .withSlaveInstanceType(InstanceType.M4_LARGE.toString())
                .withHadoopVersion("2.7.3")
                .withEc2KeyName("dspass1") // todo: change keypair
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("wordPrediction")
                .withInstances(instances)
//                .withSteps(stepDivide,stepCalcNT,stepCalcProb,stepSortResults)
                .withSteps(stepDivide)
                .withLogUri("s3n://dsp-211-ass2/logs/")
                .withServiceRole("EMR_Role")
                .withJobFlowRole("EMR_EC2_Role")
                .withReleaseLabel("emr-5.32.0");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }

    public enum Counter{N_SUM};


}
