package com.ef;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.awt.image.BufferedImageFilter;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.io.LineNumberReader;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.ef.LoadToDB.CurrentClassGetter;
import com.ef.Parser.BadCommandLine;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;
/**
 * 
 * @author mm
 * 
 * Change Log:
 * 
 *
 */
public class Parser {
	public static final String HOURLY = "hourly";
	public static final String DAILY = "daily";
	private final String[] args;
	private Date startDate;
	private String duration;
	private int threshold;
	private long startTime;
	private long endTime;
	private long durationMilli;
	private Map<String, Integer> countMap = new HashMap<String, Integer>();
	private List<String> ipsFound = new ArrayList<String>();
	private boolean loadData;
	private Connection conn;
	private String logFile;
	private List<Log> logRecords=new ArrayList<Log>();
	public final class BadCommandLine extends Exception {
		public BadCommandLine(String amessage) {
			super(amessage);
		}
	}
	public static Predicate<Log> isFlagged() {
        return x -> x.isFlagged();
    }
public class Log{
	private long id;
	private Timestamp requestTime;
	private  String ip;
	private String requestType;
	private int status;
	private String browser;
	private boolean flagged=false;
	/**
	 * @return the id
	 */
	public long getId() {
		return id;
	}
	/**
	 * @return the requestTime
	 */
	public Timestamp getRequestTime() {
		return requestTime;
	}
	/**
	 * @return the ip
	 */
	public String getIp() {
		return ip;
	}
	/**
	 * @return the requestType
	 */
	public String getRequestType() {
		return requestType;
	}
	/**
	 * @return the status
	 */
	public int getStatus() {
		return status;
	}
	/**
	 * @return the browser
	 */
	public String getBrowser() {
		return browser;
	}
	/**
	 * @param id the id to set
	 */
	public void setId(long id) {
		this.id = id;
	}
	/**
	 * @param requestTime the requestTime to set
	 */
	public void setRequestTime(Timestamp requestTime) {
		this.requestTime = requestTime;
	}
	/**
	 * @param ip the ip to set
	 */
	public void setIp(String ip) {
		this.ip = ip;
	}
	/**
	 * @param requestType the requestType to set
	 */
	public void setRequestType(String requestType) {
		this.requestType = requestType;
	}
	/**
	 * @param status the status to set
	 */
	public void setStatus(int status) {
		this.status = status;
	}
	/**
	 * @param browser the browser to set
	 */
	public void setBrowser(String browser) {
		this.browser = browser;
	}
	/**
	 * @return the flagged
	 */
	public boolean isFlagged() {
		return flagged;
	}
	/**
	 * @param flagged the flagged to set
	 */
	public void setFlagged(boolean flagged) {
		this.flagged = flagged;
	}
	
	public String toString(){
		return ip +", "+requestTime +", " + requestType+", " + status;
	}
}
public static class CurrentClassGetter extends SecurityManager {
    public String getClassName() {
                    return getClassContext()[1].getName();
    }
}


public static void usage() {
    String s = new CurrentClassGetter().getClassName();
    println(s);
    println("usage: java " + s + " --accesslog=/path/to/file --startDate=YYYY-MM-DD HH:mm:ss --duration=hourly-or-daily --threshold=a-number " + "\nExample 1:\n java  -cp parser.jar" 
    + s+" --accesslog=/path/to/file --startDate=2017-01-01.13:00:00 --duration=hourly --threshold=100 "
    		+ "\nExample 2: java "+s+"  --accesslog=/path/to/file --startDate=2017-01-01.13:00:00 --duration=hourly --threshold=100  --clean");
}
	public static void main(String[] args) {

		try {
			new Parser(args).process();
		} catch (Throwable t) {
			usage();
			
			t.printStackTrace();
		}
	}

	public Parser(String[] aargs) throws BadCommandLine {
		args = aargs;
		if (args.length < 4 || args.length > 5) {
			throw new BadCommandLine("expecting exactly 4 arguments. " + args.length);
		}
		for (int i = 0; i < args.length; i++) {
			String a = args[i];
			if(a==null){
				continue;
			}
			if ((a.startsWith("--startDate="))) {
				startDate = parseCommmandLineParameterDate(a);
			} else if (a.startsWith("--duration=")) {
				duration = parseCommmandLineParameterString(a);
			} else if (a.startsWith("--threshold=")) {
				threshold = parseCommmandLineParameterInt(a);
			}else if(a.equals("--load")){
				loadData=true;
			}else if(a.startsWith("--accesslog")){
				logFile=parseCommmandLineParameterString(a);
			}
		}
		validateCommandLineParameters();
		startTime = startDate.getTime();
		if (HOURLY.equals(duration)) {
			durationMilli = 1000 * 3600;
		} else {
			durationMilli = 1000 * 3600 * 24;
		}
		endTime = startTime + durationMilli;
		println(new Date(startTime), new Date(endTime));
	}

	public void process() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException,
			IOException, ParseException {
		conn = getConnection();
		if(loadData){
			loadData();
		}
		Statement stmt = (Statement) conn.createStatement();
		
		// ResultSet rs = stmt.executeQuery("SELECT * FROM t1");


		ResultSet rs=stmt.executeQuery("SELECT id, request_time, ip, request_type, status, browser from schema1.log");;
		while(rs.next()){
			Log log=new Log();
			log.setId(rs.getLong("id"));
			Timestamp requestTime=rs.getTimestamp("request_time");
			log.setRequestTime(requestTime);
			String ip=rs.getString("ip");
			log.setIp(ip);
			log.setStatus(rs.getInt("status"));
			log.setBrowser(rs.getString("browser"));
			log.getRequestTime().getTime();
			
			long milli=requestTime.getTime();
			if(milli >= startTime && milli <= endTime){
				log.setFlagged(true);
				Integer test = countMap.get(ip);
				if(test==null){
					countMap.put(ip, 1);
				}else{
					test++;
					countMap.put(ip, test);
				}
			}
			logRecords.add(log);
		}
		for(String ip:countMap.keySet()){
			int count=countMap.get(ip);
			if(count > threshold){
				println("found:"+ip);
				insertBlockedIP(ip);
			}
		}
		ipsFound.stream().forEach(ip -> println("ip found: " + ip));
	}
	private void insertBlockedIP(String ip) throws SQLException{
		String sql ="insert into schema1.blocked_ip(ip, comments) values(?,?)";
		PreparedStatement pst=conn.prepareStatement(sql);
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String s=formatter.format(startDate);
		pst.setString(1, ip);
		pst.setString(2, "failed threshold="+threshold+" duration="+duration +" start time="+s);
		pst.execute();
	}
	public void loadData() throws SQLException, ParseException, IOException{
		println("loading data");
		String sql="insert into schema1.log(id, request_time, ip, request_type, status, browser)"
				+ " values(?,?,?,?,?,?)";
		
		LineNumberReader lin = new LineNumberReader(
				new FileReader(logFile));
		long i=0;
		while (true) {
			String line = lin.readLine();
			if (line == null) {
				break;
			}
			String[] fields = line.split("\\|");
			// println(fields);
			String stime = fields[0];
			long milli = convertToMilli(stime);
			String ip=fields[1];
			String requestType=fields[2];
			int status=Integer.parseInt(fields[3]);
			String browser=fields[4];
			PreparedStatement pst=conn.prepareStatement(sql);
			pst.setLong(1,i);
			pst.setTimestamp(2, new Timestamp(milli));
			pst.setString(3, ip);
			pst.setString(4, requestType);
			pst.setInt(5, status);
			pst.setString(6, browser);
			pst.executeUpdate();
			i++;
		}

		
	}
	public Connection getConnection()
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		println("got driver");
		Connection conn = (Connection) DriverManager
				.getConnection("jdbc:mysql://localhost/schema1?" + "user=root&password=root");
		return conn;
	}

	public static void println(Object... a) {
		for (int i = 0; i < a.length; i++) {
			System.out.println(a[i]);
		}
	}

	private String parseCommmandLineParameter(String s) throws BadCommandLine {
		String[] a = s.split("=");

		if (a.length > 2 || a.length < 2) {
			throw new BadCommandLine("bad argument: " + s);
		}
		println(a[1]);
		return a[1];
	}

	private Date parseCommmandLineParameterDate(String s) throws BadCommandLine {
		String s2 = parseCommmandLineParameter(s);
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d;
		s2 = s2.replaceAll("\\.", " ");
		try {
			d = formatter.parse(s2);
		} catch (java.text.ParseException e) {
			e.printStackTrace();
			throw new BadCommandLine("bad parameter: " + s);
		}
		return d;
	}

	private String parseCommmandLineParameterString(String s) throws BadCommandLine {
		String s2 = parseCommmandLineParameter(s);
		return s2;
	}

	private int parseCommmandLineParameterInt(String s) throws BadCommandLine {
		String s2 = parseCommmandLineParameter(s);
		int i = -1;
		try {
			i = Integer.parseInt(s2);
		} catch (NumberFormatException e) {
			e.printStackTrace();
			throw new BadCommandLine("bad parameter: " + s);
		}
		return i;
	}

	private void validateCommandLineParameters() throws BadCommandLine {
		boolean hourlyOrDaily = HOURLY.equals(duration) || DAILY.equals(duration);
		if (!hourlyOrDaily) {
			throw new BadCommandLine("bad duration: " + duration);
		}
		if(logFile==null){
			throw new BadCommandLine("expected logfile");
		}
		File tmp=new File(logFile);
		if(!tmp.exists()){
			throw new BadCommandLine("log file doesn't exist:" +logFile);
		}
	}

	private long convertToMilli(String s) throws ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d = formatter.parse(s);
		return d.getTime();
	}
}

