# Build
mvn clean install

# Run file propagator
java -jar ./target/RequestGenerator-1.0-SNAPSHOT.jar -p /home/pc3_ceph_vm1/Downloads/full1000 /home/pc3_ceph_vm1/Downloads/op.txt > create_file_perf.txt

# Run read and write smart request generator
java -jar ./target/RequestGenerator-1.0-SNAPSHOT.jar -m /home/pc3_ceph_vm1/Downloads/full1000 > read_write_perf.txt

