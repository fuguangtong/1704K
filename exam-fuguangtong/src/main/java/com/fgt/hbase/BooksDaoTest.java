package com.fgt.hbase;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * @作者:付广通
 * @时间:2019年11月26日
 */
public class BooksDaoTest {
	public static Configuration conf;
	static{
		conf=HBaseConfiguration.create();
		//设置地址
		conf.set("hbase.zookeeper.quorum", "hadoop2");
		//设置端口号
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}
	@Test
	public void test() throws IOException {
		//查询ISBN小于1000000000的图书的书名
		scanTest("hbase_books");
	}
	@Test
	public void test1() throws IOException {
		//查询1990年到2000出版的图书信息，包括ISBN、书名和出版商（5分）
		yearBetweenTest("hbase_books");
	}
	@Test
	public void test3() throws IOException {
		//查询1990年到2000出版的图书信息，包括ISBN、书名和出版商（5分）
		countBooksByAuthor("hbase_books");
	}
	
	public static void main(String[] args) throws IOException {
		countBooksByAuthor("hbase_books");
	}
	//（7） 创建countBooksByAuthor()方法，统计统计二十世纪九十年代Kathleen Duey编著的图书总数。（5分）
	public static void countBooksByAuthor(String tableName) throws IOException {
		//获取hbase连接
		Connection connect = getConnect();
		//获取操作表对象
		HTable table = (HTable) connect.getTable(TableName.valueOf(tableName));
		//创建scan对象
		Scan scan = new Scan();
		//创建过滤器
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Year_Of_Publication"),CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("1990")));
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Year_Of_Publication"),CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("1999")));
		SingleColumnValueFilter filter3 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Book_Author"),CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("Kathleen Duey")));
		//创建过滤器集合
		FilterList filterList = new FilterList(filter1,filter2,filter3);
		//设置过滤器
		scan.setFilter(filterList);
		ResultScanner scanner = table.getScanner(scan);
		//初始化变量，用来求总图书
		int i=0;
		for (Result result : scanner) {
			//遍历累加
			i++;
		}
		System.out.println("二十世纪九十年代Kathleen Duey编著的图书总数为:"+i);
		//关闭hbase连接
		colseS(connect);
	}
	//（6） 创建yearBetweenTest()方法，查询1990年到2000出版的图书信息，包括ISBN、书名和出版商（5分）
	public static void yearBetweenTest(String tableName) throws IOException {
		//获取hbase连接
		Connection connect = getConnect();
		//获取操作表对象
		HTable table = (HTable) connect.getTable(TableName.valueOf(tableName));
		//创建scan对象
		Scan scan = new Scan();
		//创建过滤器
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Year_Of_Publication"),CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("1990")));
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Year_Of_Publication"),CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("2000")));
		//创建过滤器集合
		FilterList filterList = new FilterList(filter1,filter2);
		//设置过滤器
		scan.setFilter(filterList);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			List<Cell> listCells = result.listCells();
			for (Cell cell : listCells) {
				String row = Bytes.toString(CellUtil.cloneRow(cell));
				String Qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				String Value = Bytes.toString(CellUtil.cloneValue(cell));
				if(Qualifier.equals("Book_Title")||Qualifier.equals("Publisher")){//||Qualifier.equals("Year_Of_Publication")
					String format = String.format("INSN:%s,%s:%s", row,Qualifier,Value);
					System.out.println(format);
				}
			}
		}
		//关闭hbase连接
		colseS(connect);
	}
	//（5） 在BaseDaoTest类中创建scanTest()方法，查询ISBN小于1000000000的图书的书名，
	//输出格式为：“ISBN：0425176428，书名：What If?: The World's Foremost Military Historians Imagine What Might Have Been;Robert Cowley”
	public static void scanTest(String tableName) throws IOException {
		//获取hbase连接
		Connection connect = getConnect();
		//获取操作表对象
		HTable table = (HTable) connect.getTable(TableName.valueOf(tableName));
		//创建scan对象
		Scan scan = new Scan();
		//创建过滤器
		List<RowRange> list = new ArrayList<MultiRowRangeFilter.RowRange>();
		//查询ISBN小于1000000000的图书的书名
		RowRange rowRange = new RowRange("0", true, "1000000000", false);
		list.add(rowRange);
		MultiRowRangeFilter multiRowRangeFilter = new MultiRowRangeFilter(list);
		//设置过滤器
		scan.setFilter(multiRowRangeFilter);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			List<Cell> listCells = result.listCells();
			for (Cell cell : listCells) {
				String row = Bytes.toString(CellUtil.cloneRow(cell));
				String Qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				String Value = Bytes.toString(CellUtil.cloneValue(cell));
				if(Qualifier.equals("Book_Title")){
					String format = String.format("INSN:%s,%s:%s", row,Qualifier,Value);
					System.out.println(format);
				}
			}
		}
		//关闭hbase连接
		colseS(connect);
	}
	//获取habse连接
	public static Connection getConnect() throws IOException{
		return ConnectionFactory.createConnection(conf);
	}
	//关闭连接
	public static void colseS(Connection connection) throws IOException{
		connection.close();
	}

}
