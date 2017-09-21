package oracle.demo.oow.bd.util;

import java.io.FileInputStream;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import oracle.demo.oow.bd.constant.hbase.ConstantsHBase;

public class InitServlet extends HttpServlet {

	@Override
	public void init(ServletConfig config) throws ServletException {
		// TODO Auto-generated method stub
		
		super.init(config);
		String path;
		FileInputStream fis;
		
		
		try {
			path = InitServlet.class.getResource("/").getPath();
			fis = new FileInputStream(path+"conf.properties");
			Properties properties = new Properties();
			properties.load(fis);
			fis.close();
			System.out.println("ccccccccccccccccccccccccccccccccccccccccc");
			String outputFile = properties.getProperty("output_file");
			if(outputFile!=null){
				FileWriterUtil.OUTPUT_FILE = outputFile;
			}
			
			String zookeeper = properties.getProperty("hbase.zookeeper.quorum");
			if(zookeeper!=null){
				ConstantsHBase.ZOOKEEPER = zookeeper;
			}
			
			String hbaseRootDir = properties.getProperty("hbase.rootdir");
			if(hbaseRootDir!=null){
				ConstantsHBase.HBASE_ROOT_DIR = hbaseRootDir;
			}
			
			String mysqlUserName = properties.getProperty("mysql.username");
			if(mysqlUserName!=null){
				ConstantsHBase.MYSQL_USERNAME = mysqlUserName;
			}
			
			String mysqlPassWord = properties.getProperty("mysql.password");
			if(mysqlPassWord!=null){
				ConstantsHBase.MYSQL_PASSWORD = mysqlPassWord;
			}
			
			String mysqlURL = properties.getProperty("mysql.url");
			if(mysqlURL!=null){
				ConstantsHBase.MYSQL_URL = mysqlURL;
			}
			
			String mysqlDriver = properties.getProperty("mysql.driver");
			if(mysqlDriver!=null){
				ConstantsHBase.MYSQL_DRIVER = mysqlDriver;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}
