/*
 * Copyright 2014 Sreejith Pillai
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sreejithpillai.hbase.bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseDriver {

	private static Logger LOG = LoggerFactory.getLogger(HBaseDriver.class);
	static Configuration hbaseconfiguration = null;
	static Configuration conf = new Configuration();
	static HBaseAdmin hbaseAdmin;

	public static void connectHBase() {
		LOG.info("Initializing Connection with Hbase");
		final String HBASE_CONFIG_ZOOKEEPER_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
		final String HBASE_ZOOKEEPER_CLIENT_PORT = "2181";
		final String HBASE_CONFIG_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
		final String HBASE_ZOOKEEPER_SERVER = "1.2.3.4","172.56.12.35"; // Machine IP where
															// ZooKeeper is
															// installed
		conf.set(HBASE_CONFIG_ZOOKEEPER_CLIENT_PORT,
				HBASE_ZOOKEEPER_CLIENT_PORT);

		conf.set(HBASE_CONFIG_ZOOKEEPER_QUORUM, HBASE_ZOOKEEPER_SERVER);

		hbaseconfiguration = HBaseConfiguration.create(conf);

		try {
			hbaseAdmin = new HBaseAdmin(hbaseconfiguration);
			LOG.info("HBase connection successfull");
		} catch (MasterNotRunningException e) {
			LOG.error("HBase Master Exception " + e);
		} catch (ZooKeeperConnectionException e) {
			LOG.error("Zookeeper Exception " + e);
		}
	}

	/**
	 * Main entry point for the example.
	 * 
	 * @param args
	 *            arguments
	 * @throws Exception
	 *             when something goes wrong
	 */
	public static void main(String[] args) throws Exception {
		LOG.info("Code started");
		HBaseDriver.connectHBase(); // Initializing connection with HBase
		
		String inputPath=args[0];
		String outputPath=args[1];
		String tableName=args[2];

		conf.set("hbase.table.name", tableName);
		conf.set("xmlinput.start", "<book>");
		conf.set("xmlinput.end", "</book>");

		Job job = new Job(conf);
		job.setJarByClass(HBaseDriver.class);
		job.setJobName("Bulk Load XML into HBase");

		job.setInputFormatClass(XmlInputFormat.class);

		job.setMapperClass(HBaseMapper.class);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);

		job.setNumReduceTasks(0);

		HTable htable = new HTable(conf, tableName);
		HFileOutputFormat.configureIncrementalLoad(job, htable);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);

		// Importing the generated HFiles into a HBase table
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
		loader.doBulkLoad(new Path(args[1]), htable);
		LOG.info("Code ended");
	}

}
