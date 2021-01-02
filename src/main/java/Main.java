import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import software.amazon.awssdk.services.ec2.model.InstanceType;

public class Main {
        private static final AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());


    public static void main(String[] args) {
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        //STEP1
        HadoopJarStepConfig divide = new HadoopJarStepConfig()
                .withJar("s3://dsp-211-ass2/divideC.jar")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data","s3://dsp-211-ass2/divideCOut");
        StepConfig stepDivide = new StepConfig()
                .withName("divide")
                .withHadoopJarStep(divide)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //STEP2
        HadoopJarStepConfig calcProb = new HadoopJarStepConfig()
                .withJar("s3://dsp-211-ass2/calcProbC.jar")
                .withArgs("s3://dsp-211-ass2/divideCOut","s3://dsp-211-ass2/calcProbCOut");
        StepConfig stepCalcProb = new StepConfig()
                .withName("calcProb")
                .withHadoopJarStep(calcProb)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //STEP3
        HadoopJarStepConfig joinResults = new HadoopJarStepConfig()
                .withJar("s3://dsp-211-ass2/joinResults.jar")
                .withArgs("s3://dsp-211-ass2/divideCOut", "s3://dsp-211-ass2/calcProbCOut", "s3://dsp-211-ass2/joinResultsCOut");
        StepConfig stepJoinResults = new StepConfig()
                .withName("joinResults")
                .withHadoopJarStep(joinResults)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

		//STEP4
        HadoopJarStepConfig sortResults = new HadoopJarStepConfig()
                .withJar("s3://dsp-211-ass2/Sort.jar")
                .withArgs("s3://dsp-211-ass2/joinResultsCOut","s3://dsp-211-ass2/sortResultsCOut");
        StepConfig stepSortResults = new StepConfig()
                .withName("sortResults")
                .withHadoopJarStep(sortResults)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M4_LARGE.toString())
                .withSlaveInstanceType(InstanceType.M4_LARGE.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName("dspass1")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("wordPrediction")
                .withInstances(instances)
                .withSteps(stepDivide,stepCalcProb,stepJoinResults,stepSortResults)
                .withLogUri("s3n://dsp-211-ass2/logs/")
                .withServiceRole("EMR_Role")
                .withJobFlowRole("EMR_EC2_Role")
                .withReleaseLabel("emr-5.32.0");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }

}
