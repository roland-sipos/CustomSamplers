package mysql;

import java.util.List;

import utils.TagList;

public class MariaDBDeployer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		List<String> tagList = TagList.getTags();
		MysqlTestEnvironmentDeployer deployer =
				new MysqlTestEnvironmentDeployer("testdb-pc2.cern.ch", "3306", 
						"test", "testUser", "testPass", "", "MariaDB", tagList);

		//System.out.println("-------- MariaDB environment setup ------------");
		//deployer.deployTestEnvironment();
		//System.out.println("------- MariaDB environment teardown ----------");
		//deployer.destroyTestEnvironment();
		System.out.println("------- MariaDB environment teardown and setup -----------");
		deployer.redeployEnvironment();

	}

}
