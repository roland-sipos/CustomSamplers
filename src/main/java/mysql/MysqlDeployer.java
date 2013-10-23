package mysql;

import java.util.List;

import utils.TagList;

public class MysqlDeployer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		List<String> tagList = TagList.getTags();
		MysqlTestEnvironmentDeployer deployer =
				/*new MysqlTestEnvironmentDeployer("testdb-pc.cern.ch", "3306", 
						"testdb", "testUser", "testPass", "InnoDB", "Mysql", tagList);*/
				new MysqlTestEnvironmentDeployer("cloudnode1.cern.ch", "3306",
						"testdb", "testUser", "testPass", "InnoDB", "Mysql", tagList);

		//System.out.println("-------- MySQL environment setup ------------");
		//deployer.deployTestEnvironment();
		//System.out.println("------- MySQL environment teardown ----------");
		//deployer.destroyTestEnvironment();
		System.out.println("------- MySQL environment teardown and setup -----------");
		deployer.redeployEnvironment();

	}

}

