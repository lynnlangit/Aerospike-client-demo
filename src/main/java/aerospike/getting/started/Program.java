package aerospike.getting.started;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;

public class Program {
	
	//Update the IP addresses to the values for YOUR Aerospike instance
	private static AerospikeClient connect() {
		Host[] hosts = new Host[] { new Host("192.168.1.114", 3000),
				new Host("127.0.0.1", 3000) };
		AerospikeClient client = new AerospikeClient(new ClientPolicy(), hosts);
		return client;
	}
	
	public static void main(String[] args) {
		
		AerospikeClient client = connect();
		Policy policy = new Policy();
		WritePolicy writePolicy = new WritePolicy();
		BatchPolicy batchPolicy = new BatchPolicy();
		
		//NOTE: adjust the timeout value depending on your demo machine 
		writePolicy.timeout = 1000;
		Key key = new Key("test", "myset", "mykey");

		writeSingleValue(client, writePolicy, key);
		checkKeyExists(client, policy, key);
		addSingleValue(client, writePolicy, key);
		writeMultipleValues(client, writePolicy, key);
		writeValueWithTTL(client);

		readAllValuesForKey(client, policy, key);
		readSomeValuesForKey(client, policy, key);

		deleteValue(client, writePolicy, key);
		deleteRecord(client, writePolicy, key);

		addRecords(client, writePolicy);
		batchReadRecords(client, batchPolicy);

		multiOps(client, writePolicy, key);
		
		client.close();
	}
	
	private static void writeSingleValue(AerospikeClient client,
			WritePolicy writePolicy, Key key) {
		Bin bin = new Bin("mybin", "myReadModifyWriteValue");
		client.put(writePolicy, key, bin);
		System.out.println("Wrote this new value (or bin): "+ key);
		System.out.println("");
	}
	
	private static void checkKeyExists(AerospikeClient client, Policy policy,
			Key key) {
		System.out.println("Check a record exists");
		boolean exists = client.exists(policy, key);
		System.out.println(key + " exists? " + exists);
		System.out.println("");
	}
	
	private static void addSingleValue(AerospikeClient client,
			WritePolicy writePolicy, Key key) {
		Key newKey = new Key("test","myAddSet","myAddKey");
		Bin counter = new Bin("mybin", 1);
		client.add(writePolicy, newKey, counter);
		System.out.println("Wrote this additional value (or bin):  "+ newKey);
		System.out.println("");
	}
	
	private static void writeMultipleValues(AerospikeClient client,
			WritePolicy writePolicy, Key key) {
		Bin bin0 = new Bin("location","Oslo");
		Bin bin1 = new Bin("name", "Lynn");
		Bin bin2 = new Bin("age", 42);
		client.put(writePolicy, key, bin0, bin1, bin2);
		System.out.println("Wrote these additional values:  "+ key 
				+ " " + bin0 + " "+ bin1 + " " + bin2);
		System.out.println("");
	}
	
	private static void writeValueWithTTL(AerospikeClient client) {
		WritePolicy writePolicy = new WritePolicy();
		writePolicy.expiration = 2;
		
		Key ttlKey = new Key("test", "myTtlSet", "myTtlKey");
		Bin bin = new Bin("gender", "female");
		client.put(writePolicy, ttlKey, bin);
		
		Policy policy = new Policy();
		checkKeyExists(client, policy, ttlKey);
		System.out.println("sleeping for 4 seconds");
		try {
			Thread.sleep(4000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		checkKeyExists(client, policy, ttlKey);
		System.out.println("");
	}
	
	private static void readAllValuesForKey(AerospikeClient client, Policy policy, Key key) {
		System.out.println("Read all bins of a record");
		Record record = client.get(policy, key);
		System.out.println("Read these values: " + record);
		System.out.println("");
	}
	

	private static void readSomeValuesForKey(AerospikeClient client, Policy policy, Key key) {
		System.out.println("Read specific values (or bins) of a record");
		Record record = client.get(policy, key, "name","age");
		System.out.println("Read these values: " + record);
		System.out.println("");
	}
	
	private static void deleteValue(AerospikeClient client,
			WritePolicy writePolicy, Key key) {
		Bin bin1 = Bin.asNull("mybin");
		client.put(writePolicy, key, bin1);
		System.out.println("Deleted this value:  "+ bin1);
		System.out.println("");
	}
	
	private static void deleteRecord(AerospikeClient client,
			WritePolicy policy, Key key) {
		client.delete(policy, key);
		checkKeyExists(client, policy, key);
		System.out.println("Deleted this record: " + key);
		System.out.println("");
	}
	
	private static void addRecords(AerospikeClient client,
			WritePolicy writePolicy) {
		int size = 1024;
		for (int i = 0; i < size; i++) {
			Key key = new Key("test", "myset", (i + 1));
			client.put(writePolicy, key, new Bin("dots", i + " dots"));
		}
		System.out.println("Added " + size + " Records");
		System.out.println("");
	}
	
	private static void batchReadRecords(AerospikeClient client, BatchPolicy batchPolicy) {
		System.out.println("Batch Reads");
		int size = 1024;
		Key[] keys = new Key[size];
		for (int i = 0; i < keys.length; i++) {
			keys[i] = new Key("test", "myset", (i + 1));
		}
		Record[] records = client.get(batchPolicy, keys);
		for (int i = 0; i < records.length; i++) {
			System.out.println("Record[" + i + "]: " + records[i]);
		}
		System.out.println("");
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

}
