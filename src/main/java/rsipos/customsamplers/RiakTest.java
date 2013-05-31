package rsipos.customsamplers;

import java.util.Set;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterConfig;

public class RiakTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub	
		String[] hosts = "188.184.20.73:188.184.20.74:137.138.241.22:137.138.241.69".split(":");
		
		HTTPClientConfig.Builder configBuilder = new HTTPClientConfig.Builder();
		//HTTPClientConfig.Builder.from(HTTPClientConfig.defaults());
		configBuilder.withHost(hosts[0]);
		configBuilder.withPort(8098);
		//configBuilder.withTimeout(Integer.parseInt(getTimeout()));
		//configBuilder.withHttpClient(client)
			
		HTTPClientConfig clientConfig = configBuilder.build();			
		HTTPClusterConfig clusterConf = new HTTPClusterConfig(50);
		clusterConf.addClient(clientConfig);
		for (int i = 0; i < hosts.length; ++i) {
			System.out.println("Adding host to HTTP cluster config: " + hosts[i]);
			clusterConf.addHosts(hosts[i]);
		}
		
		try {
			IRiakClient riakClient = RiakFactory.newClient(clusterConf);
			riakClient.ping();
			Set<String> buckets = riakClient.listBuckets();
			System.out.println(buckets.toString());
			Bucket bucket = riakClient.fetchBucket("binaries").execute();
			System.out.println(bucket.getNVal());
		} catch (RiakException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
