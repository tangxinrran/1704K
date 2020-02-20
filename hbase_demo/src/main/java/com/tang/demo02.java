package com.tang;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class demo02 {

	private static Configuration conf;
	
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.91.155");
		conf.set("hbase.zookerper.property.clientPort", "2181");
	}
	
	public static void main(String[] args) throws IOException, SQLException {
		//创建表
		//createTable("hbaseApi","info");
		//存数据
		//saveData("hbaseApi", "row2", "info", "name", "张三");
		//saveEntityData("hbaseApi", "row3", "info", new student("lisi", 22, "beijing", "nan"));
		
		Connection conn = getConnection();

		HTable table = (HTable) conn.getTable(TableName.valueOf("aa"));
		
		Get get = new Get(Bytes.toBytes("row1"));
		
		Result result = table.get(get);
		
		System.out.println(Bytes.toString(result.getRow()));
		List<Cell> cells = result.listCells();
		
		for(Cell cell : cells) {
			
		}
		
		closeConnection(conn);
	}
	
	private static void scanTable() throws IOException {
		Connection conn = getConnection();

		HTable table = (HTable) conn.getTable(TableName.valueOf("lesson:hbasetest"));

		// 创建Scan对象，用于配置扫描的相关特性
		Scan scan = new Scan();

		// 通过HTable对象获得相应的扫描结果
		ResultScanner scanner = table.getScanner(scan);

		// 遍历扫描结果从每个结果中获得相应的单元格。这里的每一个结果相当于一个行，即一个rowkey所对应的所有单元素集合。
		for (Result result : scanner) {

			System.out.println(result.getRow());
			System.out.println("-----------------------------");
			// 获得当前行中所有单元格列表
			List<Cell> cells = result.listCells();

			for (Cell cell : cells) {// 遍历当前Result（行）中所有的单元格。单元格是根列族、列和值绑定在起的
//				System.out.println("rowkey:" + Bytes.toString(CellUtil.cloneRow(cell)));
				System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
				System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
				System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
				System.out.println(cell.getTimestamp()); // 获得当前单格时间戳
			}
			System.out.println("=============================");
		}

		closeConnection(conn);
	}
	
	
	//批量添加
	private static void saveDataBatch() throws IOException {
		Connection conn = getConnection();

		HTable table = (HTable) conn.getTable(TableName.valueOf("lesson:hbasetest"));

		List<Put> puts = new ArrayList<>();

		Put put = new Put(Bytes.toBytes("row4"));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("wangwu"));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("22"));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("city"), Bytes.toBytes("shenzhen"));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes("female"));

		puts.add(put);

		put = new Put(Bytes.toBytes("row5"));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhouli"));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("21"));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("city"), Bytes.toBytes("chengdu"));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes("male"));

		puts.add(put);

		table.put(puts);

		closeConnection(conn);
	}
	
	//存对象
	public static void saveEntityData(String tableName,String row,String family,student stu) throws IOException {
		Connection conn = getConnection();
		
		Table table = conn.getTable(TableName.valueOf(tableName));
		
		Put put = new Put(Bytes.toBytes(row));
		
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes("name"), Bytes.toBytes(stu.getName()));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes("age"), Bytes.toBytes(stu.getAge()));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes("city"), Bytes.toBytes(stu.getCity()));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes("sex"), Bytes.toBytes(stu.getSex()));
		
	}
	
	//存数据
	public static void saveData(String tableName,String row,String family,String qualifier,String value) throws IOException, SQLException {
		Connection conn = (Connection) getConnection();
		
		// 从连接对象中获得相应的HTable表对象
		Table table = conn.getTable(TableName.valueOf(tableName));
		
		// 创建put对象 ，用于封装要添加的数据的相关信息
		Put put = new Put(Bytes.toBytes(row));
		
		// 为Put对象添加用于插入的数据
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		
		// 调用HTable对象的put方法插入数据
		table.put(put);
		
		closeConnection(conn);
		
	}
	
	
	
	//是否存在表
	public static boolean isTableExists(String tableName) throws IOException {
		return getConnection().getAdmin().tableExists(TableName.valueOf(tableName));
		
	}
	//创建表
	public static void createTable(String tableName,String...columnFamlies) throws IOException {
		
		if(isTableExists(tableName))
			return;
		
		Admin admin = getAdmin();
		
		HTableDescriptor tdesc = new HTableDescriptor(TableName.valueOf(tableName));
		
		for (String str : columnFamlies) {
			HColumnDescriptor cDesc = new HColumnDescriptor(str);
			tdesc.addFamily(cDesc);
		}
		admin.createTable(tdesc);
	}
	
	
	//获取hbase连接工具的方法
	public static Connection getConnection() throws IOException{
		return ConnectionFactory.createConnection(conf);
		
	}
	
	//获得Admin对象
	public static Admin getAdmin() throws IOException {
		return getConnection().getAdmin();
		
	}
	
	//关闭连接
	private static void closeConnection(Connection conn) throws IOException {
		conn.close();
	}
	
}
class student{
	private String name;
	private int age;
	private String city;
	private String sex;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getSex() {
		return sex;
	}
	public void setSex(String sex) {
		this.sex = sex;
	}
	public student() {
		super();
	}
	public student(String name, int age, String city, String sex) {
		super();
		this.name = name;
		this.age = age;
		this.city = city;
		this.sex = sex;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + age;
		result = prime * result + ((city == null) ? 0 : city.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((sex == null) ? 0 : sex.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		student other = (student) obj;
		if (age != other.age)
			return false;
		if (city == null) {
			if (other.city != null)
				return false;
		} else if (!city.equals(other.city))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (sex == null) {
			if (other.sex != null)
				return false;
		} else if (!sex.equals(other.sex))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "student [name=" + name + ", age=" + age + ", city=" + city + ", sex=" + sex + "]";
	}
	
	
	
}
