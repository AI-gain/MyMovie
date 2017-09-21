package oracle.demo.oow.bd.util.hbase;

import java.io.IOException;

import oracle.demo.oow.bd.constant.hbase.ConstantsHBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 创建对应表结构
 * @author Againer
 *
 */
public class HBaseDB {

	private Connection conn;
	
	/**
	 * 获取一个连接HBase的实例
	 * @return
	 */
	public static HBaseDB getInstance(){
		
		return new HBaseDB();
	}
	
	/*
	 * 关闭hbase连接
	 */
	public void colseHBaseConn(){
		try {
			conn.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private HBaseDB(){
		
		//获取配置对象
		Configuration conf = HBaseConfiguration.create();
		//指定zookeeper
		conf.set("hbase.zookeeper.quorum",ConstantsHBase.ZOOKEEPER);
		//指定hbase rootdir
		conf.set("hbase.rootdir", ConstantsHBase.HBASE_ROOT_DIR);
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 根据表名和列族名创建表
	 * @param tableName   表名
 	 * @param columnFamilies  列族名
	 */
	public void createTable(String tableName,String[] columnFamilies){
		deleteTable(tableName);
		try {
			Admin admin = conn.getAdmin();
			//指定表名称
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			//添加列表
			for(String columFamily : columnFamilies){
				//指定列族
				HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes(columFamily));
				tableDescriptor.addFamily(columnDescriptor);
			}
			admin.createTable(tableDescriptor);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 根据表名删除已经存在的表
	 * @param tableName
	 */
	public void deleteTable(String tableName){
		
		try {
			Admin admin = conn.getAdmin();
			if(admin.tableExists(TableName.valueOf(tableName))){
				//首先disable
				admin.disableTable(TableName.valueOf(tableName));
				//drop
				admin.deleteTable(TableName.valueOf(tableName));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public Table getTableByName(String tableName){
		
		try {
			return conn.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	
}
