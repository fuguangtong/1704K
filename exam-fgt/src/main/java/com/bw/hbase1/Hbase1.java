package com.bw.hbase1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @作者:付广通
 * @时间:2019年11月25日
 */
public class Hbase1 {
	//创建config对象
	private static Configuration conf;
	static{
		conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop2");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}
	public static void main(String[] args) throws IOException {
		yearBetweenTest("analyze:hbase_books");
	}
	//（11）创建countBooksByAuthor()方法，统计统计二十世纪九十年代Kathleen Duey编著的图书总数。
	public static void countBooksByAuthor(String tableName) throws IOException{
		Connection connection = getConnection();
		HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
		//创建scan对象
		Scan scan = new Scan();
		SingleColumnValueFilter singleColumnValueFilter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Year_Of_Publication"), CompareOp.GREATER_OR_EQUAL,new BinaryComparator(Bytes.toBytes("1990")));
		SingleColumnValueFilter singleColumnValueFilter2 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Year_Of_Publication"), CompareOp.LESS,new BinaryComparator(Bytes.toBytes("2000")));
		SingleColumnValueFilter singleColumnValueFilter3 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Book_Author"), CompareOp.EQUAL,Bytes.toBytes("Kathleen Duey"));
		FilterList filterList = new FilterList(singleColumnValueFilter1,singleColumnValueFilter2,singleColumnValueFilter3);
		scan.setFilter(filterList);
		ResultScanner scanner = table.getScanner(scan);
		int i=0;
		for (Result result : scanner) {
			i++;
		}
		System.out.println("二十世纪九十年代Kathleen Duey编著的图书总数:"+i);
	}
	//（10）创建countBooksByYearTest()方法，统计二十世纪九十年代Aladdin出版商出版的图书总数。
	public static void countBooksByYearTest(String tableName) throws IOException{
		Connection connection = getConnection();
		HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
		//创建scan对象
		Scan scan = new Scan();
		SingleColumnValueFilter singleColumnValueFilter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Year_Of_Publication"), CompareOp.GREATER_OR_EQUAL,new BinaryComparator(Bytes.toBytes("1990")));
		SingleColumnValueFilter singleColumnValueFilter2 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Year_Of_Publication"), CompareOp.LESS,new BinaryComparator(Bytes.toBytes("2000")));
		SingleColumnValueFilter singleColumnValueFilter3 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Publisher"), CompareOp.EQUAL,Bytes.toBytes("Aladdin"));
		FilterList filterList = new FilterList(singleColumnValueFilter1,singleColumnValueFilter2,singleColumnValueFilter3);
		scan.setFilter(filterList);
		ResultScanner scanner = table.getScanner(scan);
		int i=0;
		for (Result result : scanner) {
			i++;
		}
		System.out.println("二十世纪九十年代Aladdin出版商出版的图书总数为:"+i);
	}
	//（9）创建publisherFilterTest()方法，查询Signet Book出版的所有图书信息，包括ISBN和书名。
	public static void publisherFilterTest(String tableName) throws IOException{
		Connection connection = getConnection();
		HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
		//创建scan对象
		Scan scan = new Scan();
		SingleColumnValueFilter singleColumnValueFilter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Publisher"), CompareOp.EQUAL,Bytes.toBytes("Signet Book"));
		scan.setFilter(singleColumnValueFilter1);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			List<Cell> listCells = result.listCells();
			for (Cell cell : listCells) {
				String row = Bytes.toString(CellUtil.cloneRow(cell));
				String Qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				if(Qualifier.equals("Book_Title") || Qualifier.equals("Publisher") ){
					String format = String.format("INBN:%s,%s:%s", row,Qualifier,value);
					System.out.println(format);
				}
			}
		}
	}
	//（8）创建nameIncludeTest()方法，查询作者名字中含有“Die”的图书信息，包括作者、书名。
	public static void nameIncludeTest(String tableName) throws IOException{
		Connection connection = getConnection();
		HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
		//创建scan对象
		Scan scan = new Scan();
		SingleColumnValueFilter singleColumnValueFilter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Book_Author"), CompareOp.EQUAL,new SubstringComparator("Die"));
		scan.setFilter(singleColumnValueFilter1);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			List<Cell> listCells = result.listCells();
			for (Cell cell : listCells) {
				String row = Bytes.toString(CellUtil.cloneRow(cell));
				String Qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				if(Qualifier.equals("Book_Title") || Qualifier.equals("Book_Author")){
					String format = String.format("%s:%s",Qualifier,value);
					System.out.println(format);
				}
			}
		}
	}
	//（7）创建yearBetweenTest()方法，查询1990年到2000出版的图书信息，包括ISBN、书名和出版商
	public static void yearBetweenTest(String tableName) throws IOException{
		Connection connection = getConnection();
		HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
		//创建scan对象
		Scan scan = new Scan();
		SingleColumnValueFilter singleColumnValueFilter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Year_Of_Publication"), CompareOp.GREATER_OR_EQUAL,new BinaryComparator(Bytes.toBytes("1990")));
		SingleColumnValueFilter singleColumnValueFilter2 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Year_Of_Publication"), CompareOp.LESS,new BinaryComparator(Bytes.toBytes("2000")));
		FilterList filterList = new FilterList(singleColumnValueFilter1,singleColumnValueFilter2);
		scan.setFilter(filterList);
		scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("Book_Title"));
		scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("Publisher"));
		scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("Year_Of_Publication"));
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			List<Cell> listCells = result.listCells();
			for (Cell cell : listCells) {
				String row = Bytes.toString(CellUtil.cloneRow(cell));
				String Qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				String format = String.format("INBN:%s,%s:%s", row,Qualifier,value);
				System.out.println(format);
				/*if(Qualifier.equals("Book_Title") || Qualifier.equals("Publisher") ||Qualifier.equals("Year_Of_Publication")){
				}*/
			}
		}
	}
	
	//（6）在BaseDaoTest类中创建scanTest()方法，查询ISBN小于1000000000的图书的书名，
	//输出格式为：“ISBN：0425176428，书名：What If?: The World's Foremost Military Historians Imagine What Might Have Been;Robert Cowley
	public static void scanTest(String tableName) throws IOException {
		Connection connection = getConnection();
		HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
		//创建scan对象
		Scan scan = new Scan();
		//设置行键范围过滤器
		List<RowRange> list=new ArrayList<MultiRowRangeFilter.RowRange>();
		RowRange rowRange = new RowRange("0", true, "1000000000", false);
		list.add(rowRange);
		MultiRowRangeFilter multiRowRangeFilter = new MultiRowRangeFilter(list);
		scan.setFilter(multiRowRangeFilter);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			List<Cell> listCells = result.listCells();
			for (Cell cell : listCells) {
				String row = Bytes.toString(CellUtil.cloneRow(cell));
				String Qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				if(Qualifier.equals("Book_Title")){
					String format = String.format("INBN:%s,%s:%s", row,Qualifier,value);
					System.out.println(format);
				}
			}
		}
	}
	//获取hbase连接
	public static Connection getConnection() throws IOException {
		return ConnectionFactory.createConnection(conf);
	}
	//获取admin
	public static Admin getAdmins() throws IOException {
		return getConnection().getAdmin();
	}
	//关闭连接
	public static void closeDown(Connection connection) throws IOException{
		connection.close();
	}
}
