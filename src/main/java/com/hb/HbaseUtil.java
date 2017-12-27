package com.hb;

import java.io.IOException;

import java.util.HashMap;

import java.util.Iterator;

import java.util.Map;

import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.HColumnDescriptor;

import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.KeyValue;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.Admin;

import org.apache.hadoop.hbase.client.Connection;

import org.apache.hadoop.hbase.client.ConnectionFactory;

import org.apache.hadoop.hbase.client.Get;

import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.client.Result;

import org.apache.hadoop.hbase.client.ResultScanner;

import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.client.Table;

import org.apache.hadoop.hbase.util.Bytes;



public class HbaseUtil {

	private static String SERIES = "s";

	private static String TABLENAME = "AF_TABLE";

	private static Connection conn;

	//private static String hbaseIp = "192.168.0.1,192.168.0.2,192.168.0.3,192.168.0.4,192.168.0.5";
	private static String hbaseIp = "10.111.134.56";

	public static void main(String[] args) throws Exception {
		init();
		// 创建表
		//createTable(TABLENAME, "");
		// 添加数据1

		String rowKey1 = "u1001c1001";

		add(rowKey1, "original_data","original_data_u1001c1001_2");

//		// 添加数据2
//
//		String rowKey2 = "u1001c1001";
//
//		Map<String, Object> columns2 = new HashMap<String, Object>();
//
//		columns2.put("original_data", "original_data_u1001c1002_1");
//
//		columns2.put("original_data", "original_data_u1001c1002_2");
//
//		add(rowKey2, columns2);
//
//		// 查询数据1-1
//
//		Map<String, String> map1 = getAllValue(rowKey1);
//
//		for (Map.Entry<String, String> entry : map1.entrySet()) {
//
//			System.out.println("map1-" + entry.getKey() + ":" + entry.getValue());
//
//		}
//
//		// 查询数据1-2
//
//		Map<String, String> map2 = getAllValue(rowKey2);
//
//		for (Map.Entry<String, String> entry : map2.entrySet()) {
//
//			System.out.println("map2-" + entry.getKey() + ":" + entry.getValue());
//
//		}
//
//		// 查询数据2
//
//		String original_data_value = getValueBySeries(rowKey1, "original_data");
//
//		System.out.println("original_data_value->" + original_data_value);
//
//		// 查看表中所有数据
//
//		getValueByTable();

	}
	
	
	public static void init() {

		Configuration config = HBaseConfiguration.create();

		config.set("hbase.zookeeper.quorum", hbaseIp);

		try {

			conn = ConnectionFactory.createConnection(config);

			createTable(TABLENAME, SERIES);

		} catch (IOException e) {

			e.printStackTrace();

		}

	}

	// 创建表

	public static void createTable(String tableName, String seriesStr) throws IllegalArgumentException, IOException {

		Admin admin = null;

		TableName table = TableName.valueOf(tableName);

		try {

			admin = conn.getAdmin();

			if (!admin.tableExists(table)) {

				System.out.println(tableName + " table not Exists");

				HTableDescriptor descriptor = new HTableDescriptor(table);

				String[] series = seriesStr.split(",");

				for (String s : series) {

					descriptor.addFamily(new HColumnDescriptor(s.getBytes()));

				}

				admin.createTable(descriptor);

			}

		} finally {

			IOUtils.closeQuietly(admin);

		}

	}

	// 添加数据

	public static void add(String rowKey, String familyKey,String familyValue) throws IOException {

		Table table = null;

		try {

			table = conn.getTable(TableName.valueOf(TABLENAME));

			Put put = new Put(Bytes.toBytes(rowKey));	
			//put.addColumn(SERIES.getBytes(), Bytes.toBytes(entry.getKey()),new ObjectAndByte().toByteArray(entry.getValue()));
			put.addColumn(SERIES.getBytes(), Bytes.toBytes(familyKey),Bytes.toBytes(familyValue));
			table.put(put);

		} finally {

			IOUtils.closeQuietly(table);

		}

	}

	// 根据rowkey获取数据

	public static Map<String, String> getAllValue(String rowKey) throws IllegalArgumentException, IOException {

		Table table = null;

		Map<String, String> resultMap = null;

		try {

			table = conn.getTable(TableName.valueOf(TABLENAME));

			Get get = new Get(Bytes.toBytes(rowKey));

			get.addFamily(SERIES.getBytes());

			Result res = table.get(get);

			Map<byte[], byte[]> result = res.getFamilyMap(SERIES.getBytes());

			Iterator<Entry<byte[], byte[]>> it = result.entrySet().iterator();

			resultMap = new HashMap<String, String>();

			while (it.hasNext()) {

				Entry<byte[], byte[]> entry = it.next();

				resultMap.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));

			}

		} finally {

			IOUtils.closeQuietly(table);

		}

		return resultMap;

	}

	// 根据rowkey和column获取数据

	public static String getValueBySeries(String rowKey, String column) throws IllegalArgumentException, IOException {

		Table table = null;

		String resultStr = null;

		try {

			table = conn.getTable(TableName.valueOf(TABLENAME));

			Get get = new Get(Bytes.toBytes(rowKey));

			get.addColumn(Bytes.toBytes(SERIES), Bytes.toBytes(column));

			Result res = table.get(get);

			byte[] result = res.getValue(Bytes.toBytes(SERIES), Bytes.toBytes(column));

			resultStr = Bytes.toString(result);

		} finally {

			IOUtils.closeQuietly(table);

		}

		return resultStr;

	}

	// 根据table查询所有数据

	public static void getValueByTable() throws Exception {

		Map<String, String> resultMap = null;

		Table table = null;

		try {

			table = conn.getTable(TableName.valueOf(TABLENAME));

			ResultScanner rs = table.getScanner(new Scan());

			for (Result r : rs) {

				System.out.println("获得到rowkey:" + new String(r.getRow()));

				for (KeyValue keyValue : r.raw()) {

					System.out.println(

							"列：" + new String(keyValue.getFamily()) + "====值:" + new String(keyValue.getValue()));

				}

			}

		} finally {

			IOUtils.closeQuietly(table);

		}

	}

	// 删除表

	public static void dropTable(String tableName) throws IOException {

		Admin admin = null;

		TableName table = TableName.valueOf(tableName);

		try {

			admin = conn.getAdmin();

			if (admin.tableExists(table)) {

				admin.disableTable(table);

				admin.deleteTable(table);

			}

		} finally {

			IOUtils.closeQuietly(admin);

		}

	}

	

}
