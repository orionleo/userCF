package userCF_Package;

import java.io.IOException;

import userCF_Step1.userCF_Step1_main;
import userCF_Step2.userCF_Step2_main;
import userCF_Step3.userCF_Step3_main;
import userCF_Step4.userCF_Step4_main;
import userCF_Step5.userCF_Step5_main;

public class JobRunner {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		
		new userCF_Step1_main();
		userCF_Step1_main.main(args);
		
		new userCF_Step2_main();
		userCF_Step2_main.main(args);
		
		new userCF_Step3_main();
		userCF_Step3_main.main(args);
		
		new userCF_Step4_main();
		userCF_Step4_main.main(args);
		
		new userCF_Step5_main();
		userCF_Step5_main.main(args);
	}
}
