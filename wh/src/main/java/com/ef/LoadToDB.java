package com.ef;

import java.io.File;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import com.ef.Parser.BadCommandLine;
import com.ef.Parser.Log;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

public class LoadToDB {
	private final String[] args;

	private Connection conn;
	private String logFile;
	private boolean clean;
	public final class BadCommandLine extends Exception {
		public BadCommandLine(String amessage) {
			super(amessage);
		}
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
	public static void main(String[] args) {

		try {
			new LoadToDB(args).process();
		} catch (Throwable t) {
			t.printStackTrace();
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
        println("usage: java " + s + " log-file-to-input [--clean]" + "\nExample 1:\n java " 
        + s+" /Users/mm/Downloads/wallethub/Java_MySQL_Test/access.log "
        		+ "\nExample 2: java "+s+"  /Users/mm/Downloads/wallethub/Java_MySQL_Test/access.log --clean");
}
	public LoadToDB(String[] aargs) throws BadCommandLine {
		args = aargs;
		if (args.length < 1 || args.length > 2) {
			usage();
			System.exit(1);;

		}
		logFile=args[0];
		if(args.length==2 )
			if("--clean".equals(args[1])){
				clean=true;
			}else{
				println("expected second argument to be --clean or blank.");
				usage();
				System.exit(1);;
			}

	}

	public void process() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException,
			IOException, ParseException {
		getConnection();
		println("connection="+conn);
		if(clean){
			clean();
		}
		loadData();
		long rcount=getRecordCount();
		println("record count after load="+rcount);
	}
	public void clean() throws SQLException{
		String sql="DELETE  FROM schema1.log";
		conn.createStatement().execute(sql);
		long rcount=getRecordCount();
		println("record count after delete="+rcount);
	}
	public long getRecordCount() throws SQLException{
		ResultSet rs=conn.createStatement().executeQuery("SELECT count(*) FROM schema1.log");
		long rcount=-1;
		if(rs.next()){
			rcount=rs.getLong(1);
		}
		return rcount;
	}
	public void loadData() throws SQLException, ParseException, IOException{
		println("loading data");
		String sql="insert into schema1.log( request_time, ip, request_type, status, browser)"
				+ " values(?,?,?,?,?)";
		
		LineNumberReader lin = new LineNumberReader(
				new FileReader(logFile));
		long i=1;
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
			pst.setTimestamp(1, new Timestamp(milli));
			pst.setString(2, ip);
			pst.setString(3, requestType);
			pst.setInt(4, status);
			pst.setString(5, browser);
			println(i);
			pst.executeUpdate();
			i++;
		}

		
	}
	public Connection getConnection()
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		println("got driver");
	    conn = (Connection) DriverManager
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

	private long convertToMilli(String s) throws ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d = formatter.parse(s);
		return d.getTime();
	}
	private void validateCommandLineParameters() throws BadCommandLine {

		if(logFile==null){
			throw new BadCommandLine("expected logfile");
		}
		File tmp=new File(logFile);
		if(!tmp.exists()){
			throw new BadCommandLine("log file doesn't exist:" +logFile);
		}
	}
}
