# oap-aerospike

Thin (simple) AeroSpike client. Used for async operations in [AeroSpike](https://docs.aerospike.com/docs/) 
by using [AerospikeClient#operations](oap-aerospike/src/main/java/oap/aerospike/AerospikeClient.java) and
[AerospikeAsyncClient](oap-aerospike/src/main/java/oap/aerospike/AerospikeAsyncClient.java) respectively. 
Two services are configured based on AeroSpike client: _aerospike-client-reader_ and _aerospike-client-writer_ in 
[oap-module.conf](oap-aerospike/src/main/resources/META-INF/oap-module.conf) 

AeroSpike logging is created by [AerospikeLog](oap-aerospike/src/main/java/oap/aerospike/AerospikeLog.java) 
and configured by _aerospike-log_ service in [oap-module.conf](oap-aerospike/src/main/resources/META-INF/oap-module.conf).

Use [AerospikeFixture](oap-aerospike/src/main/java/oap/aerospike/AerospikeFixture.java) for any tests based on the AeroSpike 
server. See [AerospikeClientTest](oap-aerospike/src/test/java/oap/aerospike/AerospikeClientTest.java) for any specific test cases.