package com.jaccson;

import java.util.HashSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BSON;


import com.mongodb.BasicDBList;
import com.mongodb.DBObject;



public class BuildIndexMR implements Tool {

	public static class BIMapper extends Mapper<Key, Value, Text, Text> {

		private void indexJSON(Object obj, String prefix, String rowid) throws MutationsRejectedException {

			if(obj instanceof DBObject) {
				
				DBObject jobj = (DBObject) obj;

				for(String name : jobj.keySet()) {
					Object o = jobj.get(name);
					
					indexJSON(o, prefix + name + ".", rowid);
				}
			}
			
			else if (obj instanceof BasicDBList) {
				
				BasicDBList oarr = (BasicDBList)obj;
				
				// index under the same name?
				for(int i=0; i < oarr.size(); i++) {
					indexJSON(oarr.get(i), prefix, rowid);
				}
			}
			
			else if (obj instanceof Double || obj instanceof Integer || obj instanceof String) {
				
				if(prefix.endsWith("."))
					prefix = prefix.substring(0, prefix.length() -1);
				
				// check to see whether this field is indexed
				if(!prefix.equals(indexField)) {
					return;
				}
				
				if(obj instanceof String) {
					// index individual words
					HashSet<String> words = new HashSet<String>();
					for(String s : ((String) obj).split("\\s+")) {
						words.add(s.toLowerCase());
					}
					for(String word: words) {
						if(word.length() == 0)
							continue;
						
						byte[] bytes = IndexHelper.indexValueForObject(word);
						
						Mutation m = new Mutation(new Text(bytes));
						m.put(tableName + "_" + prefix, rowid, BLANK_VALUE);
					
						indexWriter.addMutation(m);
					}
				}
				else {
					byte[] bytes = IndexHelper.indexValueForObject(obj);
				
				
					Mutation m = new Mutation(new Text(bytes));
					m.put(tableName + "_" + prefix, rowid, BLANK_VALUE);
				
					indexWriter.addMutation(m);
				}
			}
		}
		
		private String indexField;
		private String tableName;
		private BatchWriter indexWriter;
		private static Value BLANK_VALUE = new Value("".getBytes());
		
		public void setup(Context c) {
			Configuration conf = c.getConfiguration();
			indexField = conf.get("acc.index_field");
			tableName = conf.get("acc.table_name");
			
			Instance inst = new ZooKeeperInstance(conf.get("acc.instance"), conf.get("acc.zkservers"));
			try {
				Connector conn = inst.getConnector(conf.get("acc.username"), conf.get("acc.password").getBytes());
				indexWriter = conn.createBatchWriter(tableName + "_" + indexField.replace('.', '_'), 1000000L, 1000L, 10);
			} catch (AccumuloException e) {

				e.printStackTrace();
			} catch (AccumuloSecurityException e) {

				e.printStackTrace();
			} catch (TableNotFoundException e) {

				e.printStackTrace();
			}
			
		}
		
		public void map(Key k, Value v, Context c) {
			
			try {
				DBObject obj = (DBObject) BSON.decode(v.get());
				
				indexJSON(obj, "", k.getRow().toString());
				
			} catch (MutationsRejectedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public void cleanup(Context context) {
			try {
				indexWriter.close();
			} catch (MutationsRejectedException e) {
				e.printStackTrace();
			}
		}
	}

	private Configuration conf = null;


	public Configuration getConf() {
		if(conf == null) 
			conf = new Configuration();

		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		
		Job job = new Job(getConf());
		
		job.setJarByClass(BuildIndexMR.class);
		job.setMapperClass(BuildIndexMR.BIMapper.class);
		job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		Path outputPath = new Path("/tmp/build_index_" + args[6]);
		FileSystem.get(conf).delete(outputPath, true);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		
		// configure input parameters
		job.setInputFormatClass(AccumuloInputFormat.class);
		
		AccumuloInputFormat.setZooKeeperInstance(job, args[1], args[2]);
		AccumuloInputFormat.setInputInfo(job, args[3], args[4].getBytes(), args[5], new Authorizations());
		
		Configuration conf = job.getConfiguration();
		conf.set("acc.instance", args[1]);
		conf.set("acc.zkservers", args[2]);
		conf.set("acc.username", args[3]);
		conf.set("acc.password", args[4]);
		conf.set("acc.table_name", args[5]);
		conf.set("acc.index_field", args[6]);
		
		// run
		job.waitForCompletion(args[7].equals("block"));
		
		FileSystem.get(conf).delete(outputPath, true);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new BuildIndexMR(), args);
	}
}
