package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.jruby.runtime.Constants;
import org.junit.Test;

import oracle.demo.oow.bd.constant.KeyConstant;
import oracle.demo.oow.bd.constant.hbase.ConstantsHBase;
import oracle.demo.oow.bd.dao.CustomerDAO;
import oracle.demo.oow.bd.pojo.ActivityType;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.GenreMovieTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.FileWriterUtil;
import oracle.demo.oow.bd.util.KeyUtil;
import oracle.demo.oow.bd.util.hbase.HBaseDB;
import oracle.demo.oow.bd.util.hbase.OperateList;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

public class ActivityDao {
	
	/**
	 * 通过csutId和movieId获取用户选择播放的电影的状态信息
	 * @param custId
	 * @param movieId
	 * @return
	 * @throws IOException
	 */
	public ActivityTO getActivityTO(int custId,int movieId) throws IOException{
		
		 ActivityTO activityTO = new ActivityTO();
		 HBaseDB hbaseDB = HBaseDB.getInstance();
			Table table = hbaseDB.getTableByName(ConstantsHBase.TABLE_ACTIVITY);
			if(table!=null){
				Scan scan = new Scan();
				//定义FilterList
				FilterList filterList = new FilterList();
				//定义单值过滤器过滤当前用户
				Filter userFilter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_CUSTOMER_ID), 
						CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(custId)));
				//定义单值过滤器过滤当前电影
				Filter movieFilter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID), 
						CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(movieId)));
				//定义单值过滤器过滤用户活动
				Filter activityFilter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY), 
						CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(ActivityType.PAUSED_MOVIE.getValue())));
				filterList.addFilter(userFilter);
				filterList.addFilter(movieFilter);
				filterList.addFilter(activityFilter);
				
				scan.setFilter(filterList);
				ResultScanner resultScanner = table.getScanner(scan);
				for (Result result : resultScanner) {
					if(result!=null){
						activityTO.setPosition(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID))));
					}
					else
					{
						activityTO = null;
					}
					break;
				}
			}
			table.close();
			hbaseDB.colseHBaseConn();
		 return activityTO;
	}
	
	/**
	 * 获取用户已经看完的电影列表
	 * @param custId
	 * @return
	 * @throws IOException
	 */
	public List<MovieTO> getCustomerHistoricWatchList(int custId) throws IOException{
		return getMovieTOListByActivity(custId, ActivityType.COMPLETED_MOVIE.getValue());
	}
	
	/**
	 * 获取用户暂停的电影列表信息
	 * @param custId
	 * @param movieI
	 * @return
	 * @throws IOException
	 */
	public List<MovieTO> getCustomerCurrentWatchList(int custId) throws IOException{
		return getMovieTOListByActivity(custId,ActivityType.PAUSED_MOVIE.getValue());
	}
	
	/**
	 * 获取用户浏览过的电影
	 * @param custId
	 * @return
	 * @throws IOException
	 */
	public List<MovieTO> getCustomerBrowseList(int custId) throws IOException{
		return getMovieTOListByActivity(custId, ActivityType.BROWSED_MOVIE.getValue());
	}
	
	/**
	 * 通过用户id、活动状态、电影id查询电影实体
	 * @param userId
	 * @param activity
	 * @return
	 * @throws IOException
	 */
	public List<MovieTO> getMovieTOListByActivity(int userId,int activity) throws IOException{
		
		List<MovieTO> movieTOList = new ArrayList<>();
		//首先通过activity表通过用户id查询用户浏览过的电影id列表
		List<String> movieIdList = new ArrayList<>();
		
		HBaseDB hbaseDB = HBaseDB.getInstance();
		Table table = hbaseDB.getTableByName(ConstantsHBase.TABLE_ACTIVITY);
		if(table!=null){
			Scan scan = new Scan();
			//定义FilterList
			FilterList filterList = new FilterList();
			//定义单值过滤器过滤当前用户
			Filter userFilter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_CUSTOMER_ID), 
					CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(userId)));
			//定义单值过滤器过滤用户活动
			Filter activityFilter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY), 
					CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(activity)));
			filterList.addFilter(userFilter);
			filterList.addFilter(activityFilter);
			
			scan.setFilter(filterList);
			ResultScanner resultScanner = table.getScanner(scan);
			for (Result result : resultScanner) {
				movieIdList.add(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID)))+"");
			}
			System.out.println(movieIdList);
			//movieIdList.add(180+"");
			if(movieIdList.size()!=0){
				OperateList operateList = new OperateList();
				operateList.RemoveListValueRepeat(movieIdList);
			}
			//movieIdList.add("857");
			System.out.println(movieIdList);
		}
		table.close();
		hbaseDB.colseHBaseConn();
		
		//去除list中重复的movieId
		
		//迭代movieIdList获取movieId，通过movieId获取MovieTO实体
		MovieDao movieDao = new MovieDao();
		Iterator<String> iter = movieIdList.iterator();
		while(iter.hasNext()){
			MovieTO movieTO = movieDao.getMovieById(iter.next());
			movieTOList.add(movieTO);
		}
		return movieTOList;
	}

	
	/**
	 * 向用户活动（状态表）插入数据
	 * @param activityTO
	 * @throws IOException 
	 */
	public void insertCustomerActivity(ActivityTO activityTO) throws IOException {
		// TODO Auto-generated method stub
        ActivityType activityType = null;
        String jsonTxt = null;

        //CustomerDAO customerDAO = new CustomerDAO();
        CustomerDao customerDao = new CustomerDao();

        if (activityTO != null) {
            jsonTxt = activityTO.getJsonTxt();
            System.out.println("User Activity| " + jsonTxt);
            
                     
                
            /**
             * This system out should write the content to the application log
             * file.
             */
            //将用户活动写入日志文件
            FileWriterUtil.writeOnFile(activityTO.getActivityJsonOriginal().toString());
            
            //根据用户活动类型将用户行为写入Activity表
            String rowkey=null;
            Put put = null;
            HBaseDB hbaseDB = HBaseDB.getInstance();
            Table table = hbaseDB.getTableByName(ConstantsHBase.TABLE_ACTIVITY);
            activityType = activityTO.getActivity();
            
            //如果用户活动类型为 RATE_MOVIE(1),COMPLETED_MOVIE(2), PAUSED_MOVIE(3),STARTED_MOVIE(4), BROWSED_MOVIE(5)
            if(activityType.getValue()<=5){
            	System.out.println(activityType.getValue());
            	rowkey = createActivityIdFormGid();
            	put = new Put(Bytes.toBytes(rowkey));
            	
            	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_CUSTOMER_ID), 
            			Bytes.toBytes(activityTO.getCustId()));
            	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID), 
            			Bytes.toBytes(activityTO.getMovieId()));
            	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID), 
            			Bytes.toBytes(activityTO.getGenreId()));
            	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY), 
            			Bytes.toBytes(activityTO.getActivity().getValue()));
            	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RECOMMENDED), 
            			Bytes.toBytes(activityTO.isRecommended().toString()));
            	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TIME), 
            			Bytes.toBytes(activityTO.getFormattedTime()));
            	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING), 
            			Bytes.toBytes(activityTO.getRating().getValue()));
            	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_PRICE), 
            			Bytes.toBytes(activityTO.getPrice()));
            	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_POSITION), 
            			Bytes.toBytes(activityTO.getPosition()));
            	table.put(put);
            }
            table.close();
            hbaseDB.colseHBaseConn();

        } //if (activityTO != null)
	}//insertCustomerActivity

	/**
	 * 为activity根据计数器自增创建行键
	 * @return
	 * @throws IOException 
	 */
	private String createActivityIdFormGid() throws IOException {
		// TODO Auto-generated method stub
		String rowkey = "";
		HBaseDB hbaseDB = HBaseDB.getInstance();
		Table table = hbaseDB.getTableByName(ConstantsHBase.TABLE_GID);
		
		//首先获取gid表的最后一行信息(从中获取行键和列族gid下activityid的值)
		String gid = null;
		long activityId = 0;
		Scan scan = new Scan();
		Filter filter = new PageFilter(1);
		scan.setFilter(filter);
		//倒序扫描,获取最后一行的行键和activityId列对应
		scan.setReversed(true);
		ResultScanner resultScanner = table.getScanner(scan);
		for (Result result : resultScanner) {
			gid = Bytes.toString(result.getRow());
			activityId = Bytes.toLong(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_GID_GID), Bytes.toBytes(ConstantsHBase.QUALIFIER_GID_ACTIVITY_ID)));
		}
		
		
		if(gid!=null){
			//将计数器的值作为activityId列的值和行键的值，新增一列值到数据库
			Put put_new = new Put(Bytes.toBytes((activityId+1)+""));
			put_new.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_GID_GID), Bytes.toBytes(ConstantsHBase.QUALIFIER_GID_ACTIVITY_ID), Bytes.toBytes(activityId+1));
			table.put(put_new);
			rowkey = (activityId+1)+"";
		
		}
		//如果扫描结果为空，则表明gid表为空，为gid新添加一条数据项
		else
		{
			Put put = new Put(Bytes.toBytes("1"));
			long firstId = 1;
			put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_GID_GID), Bytes.toBytes(ConstantsHBase.QUALIFIER_GID_ACTIVITY_ID), Bytes.toBytes(firstId));
			table.put(put);
			rowkey = "1";
		}
		
		table.close();
		hbaseDB.colseHBaseConn();
		System.out.println(rowkey);
		return rowkey;
	}

	@Test
	public void testCreateActivityIdFormGid() throws IOException{
		ActivityDao activityDao = new ActivityDao();
		activityDao.createActivityIdFormGid();
	}
	
	@Test
	public void testGetCustomerBrowseList() throws IOException{
		ActivityDao activityDao = new ActivityDao();
		activityDao.getMovieTOListByActivity(1255601, ActivityType.BROWSED_MOVIE.getValue());
	}
}
