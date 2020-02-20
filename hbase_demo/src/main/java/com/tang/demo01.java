package com.tang;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class demo01 {

	public static void main(String[] args) throws IOException {
		//创建配置对象
		 Configuration conf = HBaseConfiguration.create();
		 
		//配置连接
		conf.set("hbase.zookeeper.quorum", "192.168.91.155");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
		//获取HBASE的链接
		Connection conn = ConnectionFactory.createConnection(conf);
		
		//获取Admin对象
		Admin admin = conn.getAdmin();
		
		//检查是否存在某个表
		System.out.println("是否存在:"+ admin.tableExists(TableName.valueOf("aa")));
		
		//关闭HBASE的连接
		conn.close();
		
	}
}
