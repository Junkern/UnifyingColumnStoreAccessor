package implementations;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;


public class CassandraHandler {

	static Cluster cluster;
	static Session session;
	
	public static void connectToDatabase(String address) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		session = cluster.connect();
	}
}
