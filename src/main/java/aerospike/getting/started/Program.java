package aerospike.getting.started;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class Program {
	
	//Update the IP addresses to the values for your Aerospike instance
	private static AerospikeClient connect() {
		Host[] hosts = new Host[] { new Host("192.168.1.114", 3000),
				new Host("127.0.0.1", 3000) };
		AerospikeClient client = new AerospikeClient(new ClientPolicy(), hosts);
		return client;
	}
	
	public static void main(String[] args) {
		
		AerospikeClient client = connect();
		WritePolicy writePolicy = new WritePolicy();
		
		//NOTE: you may want to adjust the timeout value depending on your demo machine 
		writePolicy.timeout = 1000;
		Key key = new Key("test", "myset", "mykey");

		writeSingleValue(client, writePolicy, key);
		writeMultipleValues(client, writePolicy, key);
		deleteBin(client, writePolicy, key);
		ttl(client);

		Policy policy = new Policy();
		readKey(client, policy, key);
		readBins(client, policy, key);
		checkExists(client, policy, key);

		deleteRecord(client, writePolicy, key);

		addRecords(client, writePolicy);
		batchReads(client, policy);

		multiOps(client, writePolicy, key);

		client.close();
	}

	private static void addRecords(AerospikeClient client,
			WritePolicy writePolicy) {
		int size = 1024;
		for (int i = 0; i < size; i++) {
			Key key = new Key("test", "myset", (i + 1));
			client.put(writePolicy, key, new Bin("dots", i + " dots"));
		}
	}
	
	private static void writeSingleValue(AerospikeClient client,
			WritePolicy writePolicy, Key key) {
		Bin bin = new Bin("mybin", "myvalue");
		client.put(writePolicy, key, bin);
	}
	
	private static void writeMultipleValues(AerospikeClient client,
			WritePolicy writePolicy, Key key) {
		Bin bin2 = new Bin("age", 42);
		Bin bin1 = new Bin("name", "Lynn");
		client.put(writePolicy, key, bin1, bin2);
	}
	
	private static void deleteRecord(AerospikeClient client,
			WritePolicy policy, Key key) {
		System.out.println("Delete a Record");
		client.delete(policy, key);
		checkExists(client, policy, key);
	}
	
	private static void deleteBin(AerospikeClient client,
			WritePolicy writePolicy, Key key) {
		Bin bin1 = Bin.asNull("mybin");
		client.put(writePolicy, key, bin1);
	}

	private static void multiOps(AerospikeClient client,
			WritePolicy writePolicy, Key key) {
		System.out.println("Multiops");
		Bin bin1 = new Bin("optintbin", 7);
		Bin bin2 = new Bin("optstringbin", "string value");
		client.put(writePolicy, key, bin1, bin2);
		Bin bin3 = new Bin(bin1.name, 4);
		Bin bin4 = new Bin(bin2.name, "new string");
		Record record = client.operate(writePolicy, key, Operation.add(bin3),
				Operation.put(bin4), Operation.get());
		System.out.println("Record: " + record);
	}
	
	private static void ttl(AerospikeClient client) {
		WritePolicy writePolicy = new WritePolicy();
		writePolicy.expiration = 2;

		Key key = new Key("test", "myset", "mykey2");
		Bin bin = new Bin("gender", "female");
		client.put(writePolicy, key, bin);

		Policy policy = new Policy();
		checkExists(client, policy, key);
		System.out.println("sleeping for 4 seconds");
		try {
			Thread.sleep(4000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		checkExists(client, policy, key);

	}

	private static void batchReads(AerospikeClient client, Policy policy) {
		System.out.println("Batch Reads");
		int size = 1024;
		Key[] keys = new Key[size];
		for (int i = 0; i < keys.length; i++) {
			keys[i] = new Key("test", "myset", (i + 1));
		}
		Record[] records = client.get(policy, keys);
		for (int i = 0; i < records.length; i++) {
			System.out.println("Record[" + i + "]: " + records[i]);
		}
	}

	private static void checkExists(AerospikeClient client, Policy policy,
			Key key) {
		System.out.println("Check a record exists");
		boolean exists = client.exists(policy, key);
		System.out.println(key + " exists? " + exists);
	}

	private static void readKey(AerospikeClient client, Policy policy, Key key) {
		System.out.println("Read all bins of a record");
		Record record = client.get(policy, key);
		System.out.println("Record: " + record);
	}

	private static void readBins(AerospikeClient client, Policy policy, Key key) {
		System.out.println("Read specific bins of a record");
		Record record = client.get(policy, key, "name");
		System.out.println("Record: " + record);
	}
}
