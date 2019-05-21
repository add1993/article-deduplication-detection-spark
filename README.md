# Large Scale Data Collection and Preprocessing and Running Deduplication Detection in Spark

1. Firstly, we need to extract news data using newsplease. Some sample data is already provided in the sample_data folder.

2. To extract data with newsplease use the sitelist.hjson and specify the URL needed to be fetched. Copy the sitelist.hjson to the newsplease config folder inside it's installation directory.
Config folder : /news-please-repo/config

3. Run newsplease from the command line by using "news-please" command and it will start collecting data. The data will be collected in the data folder parallel to the config folder.
Crawled Data : /news-please-repo/data

4. Now, use the stream_producer.py file to create Kafka stream. Please, add the basePath for the newsplease stored data. This script will read the json files and publish it on topic 'test' for Spark Streaming.

5. Run the deduplication_stream.py by using spark-submit. Make sure mongodb is running and it has a database with name "Deduplication"  and a collection inside it with name "deduplication_collection". This script will read the kafka stream data through topic 'test'. This data will be processed and matched in the mongodb database.


Windows Commands :

start zookeeper : bin/zookeeper-server-start.sh config/zookeeper.properties

start kafka server : bin/kafka-server-start.sh config/server.properties

submit code : ./spark-submit.cmd  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.1 deduplication_stream.py 

Starting zookeeper and kafka command is same for linux. For submitting code for linux use spark-submit.

NOTE : Please use the required UDPIPE file depending on the laguage. e.g. english-ewt-ud-2.3-181115.udpipe
