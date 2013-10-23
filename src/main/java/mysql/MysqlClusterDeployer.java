package mysql;

import java.util.List;

import utils.TagList;

public class MysqlClusterDeployer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		List<String> tagList = TagList.getTags();
		MysqlTestEnvironmentDeployer deployer =
				new MysqlTestEnvironmentDeployer("mysqlclustermng.cern.ch", "3306", 
						"testdb", "testUser", "testPass", "", "Mysql", tagList);

		//System.out.println("-------- MySQL-Cluster environment setup ------------");
		//deployer.deployTestEnvironment();
		//System.out.println("------- MySQL-Cluster environment teardown ----------");
		//deployer.destroyTestEnvironment();
		System.out.println("------- MySQL-Cluster environment teardown and setup -----------");
		deployer.redeployEnvironment();

	}

}
