package edu.mki.bachelor.dataimport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.commons.math3.ml.clustering.MultiKMeansPlusPlusClusterer;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Charsets;

/**
 * 
 * load data local infile 'tripleList.txt' into table wfneu CHARACTER SET 'latin1' FIELDS TERMINATED BY ';' (wort,frequenz,datum);
 *
 */

/**
 * 
 * @author marcel
 * 
 */
public class DataImporterImpl implements DataImporter {

	// DB-Daten
	private String dbHost = "woclu3.informatik.uni-leipzig.de";
	private String dbPort = "3306";
	private String newDBName = "mkis_bachelor";
	private String dbUsername = "mkisilowski";
	private String dbPassword = "D,hdn1-Rd?";
	// DB-Daten Ende
	private List<String> wordList;
	private ArrayList<String> dataBaseList;
	private PreparedStatement pStmt;

	private PreparedStatement preparedSelectStatement;

	private Connection databaseConnection;

	private ArrayList<Triple<String, Double, String>> tripleList = new ArrayList<Triple<String, Double, String>>();

	public ArrayList<String> getWDTDatabases() {
		ArrayList<String> tmp = new ArrayList<String>();
		try {
			setDatabase("information_schema");
			Statement stmt = getDatabaseConnection().createStatement();
			ResultSet rs = stmt
					.executeQuery("select SCHEMA_NAME from schemata where SCHEMA_NAME regexp 'wdt[0-9]{8}$';");
			while (rs.next()) {
				tmp.add(rs.getString(1));
			}
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.dataBaseList = tmp;
		return tmp;
	}

	public void connctToSQL() {
		String url = "jdbc:mysql://" + dbHost + ":" + dbPort;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			setDatabaseConnection(DriverManager.getConnection(url, dbUsername,
					dbPassword));
			System.out.println("SQL-Connection established");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void closeSQLConnection() {
		if (getDatabaseConnection() != null) {
			try {
				getDatabaseConnection().close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void setDatabase(String databaseName) {
		try {
			getDatabaseConnection().setCatalog(databaseName);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private String createSelectStmt() {
		return null;

	}

	public List<String> getWordList() {
		return wordList;
	}

	public void setWordList(List<String> list) {
		this.wordList = list;
	}

	public String generateDateStringFromDBName(String dataBaseName) {
		StringBuilder sb = new StringBuilder();
		sb.append(dataBaseName.substring(3, 7) + "-")
				.append(dataBaseName.substring(7, 9) + "-")
				.append(dataBaseName.substring(9, 11));
		return sb.toString();

	}

	public int getDerCount() {
		int derCount = 0;
		try {
			Statement stmt = getDatabaseConnection().createStatement();
			ResultSet rs = stmt
					.executeQuery("select anzahl from wortliste where wort_alph=\'der\';");
			while (rs.next()) {
				derCount += rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return derCount;
	}

	public int getAllWordsCount() {
		int allWords = 0;
		try {
			Statement stmt = getDatabaseConnection().createStatement();
			ResultSet rs = stmt
					.executeQuery("select sum(anzahl) from wortliste;");
			while (rs.next()) {
				allWords += rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return allWords;
	}

	public List<String> getSignificantWordsForWordList(List<String> wordList) {
		HashSet<String> result = new HashSet<String>();
		int i = 1;
		Queue<Long> window = new LinkedList<Long>();
		int period = 5;
		long sum = 0;
		Date time;
		for (String db : dataBaseList) {
			time = new Date();
			if (i == 600)
				break;
			setDatabase(db);
			System.out.println(i + "/" + dataBaseList.size());
			// EXTEND WORDLIST WITH SIGNIFICANT WORDS
			// TODO extend
			result.addAll(extendWordListWithSigWords(wordList));

			Long nTime = new Date().getTime() - time.getTime();
			sum += nTime;
			window.add(nTime);
			if (window.size() > period) {
				sum -= window.remove();
			}
			System.out.println("WordList Time till finish:"
					+ ((sum / window.size()) * 1.0 / 1000)
					* (dataBaseList.size() - i));

			i++;
		}
		return new ArrayList(result);
	}

	public void close() {
		try {
			getDatabaseConnection().close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public HashMap<String, Integer> executeSelectQuery(String query) {
		HashMap<String, Integer> result = new HashMap<String, Integer>();
		try {
			Statement stmt = getDatabaseConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				result.put(rs.getString(1), rs.getInt(2));
			}
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	public int executeInsertQuery(String query) {
		int count = 0;
		try {
			Statement stmt = getDatabaseConnection().createStatement();
			count = stmt.executeUpdate(query);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return count;
	}

	public String generateSelectQuery(List<String> wordList) {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT wort_bin,anzahl FROM wortliste WHERE wort_bin IN (");
		for (String word : wordList) {
			word = word.replace("'", "");
			sb.append("\'" + word + "\'");
			// Word is last word in list finish query
			if (wordList.get(wordList.size() - 1) == word) {
				sb.append(");");
			} else
				sb.append(",");
		}
		System.out.println(sb.toString());
		return sb.toString();
	}

	public List<String> extendWordListWithSigWords(List<String> wordList2) {
		Set<Integer> tmp = new HashSet<>();
		Set<String> result = new HashSet<String>(wordList2);
		try {
			Statement stmt = getDatabaseConnection().createStatement();
			ResultSet rs = stmt.executeQuery(generateSelectWordNr(wordList2));
			while (rs.next()) {
				tmp.add(rs.getInt(1));
			}
			rs.close();
			if (tmp.size() > 0) {
				rs = stmt
						.executeQuery(generateSelectSigWords(new ArrayList<Integer>(
								tmp)));
				while (rs.next()) {
					result.add(rs.getString(1));
				}
			}
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return new ArrayList<String>(result);
	}

	private String generateSelectWordNr(List<String> wordList2) {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT wort_nr FROM wortliste WHERE wort_alph IN (");
		for (String word : wordList2) {
			sb.append("\'" + word + "\'");
			// Word is ast word in list finish query
			if (wordList2.get(wordList2.size() - 1) == word) {
				sb.append(");");
			} else
				sb.append(",");
		}
		return sb.toString();
	}

	private String generateSelectSigWords(ArrayList<Integer> wordList) {
		StringBuilder sb = new StringBuilder();
		sb.append("select w.wort_bin from wortliste as w,kollok_sig as k WHERE k.wort_nr2=w.wort_nr AND k.wort_nr1 IN (");
		for (int wordNr : wordList) {
			sb.append(wordNr);
			// Word is ast word in list finish query
			if (wordList.get(wordList.size() - 1) == wordNr) {
				sb.append(");");
			} else
				sb.append(",");
		}
		return sb.toString();
	}

	public String generateInsertQuery(HashMap<String, Integer> wordList,
			String date, int derCount) {
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT IGNORE INTO wfneu values");
		int i = 0;
		Set<String> tmp = new HashSet<String>(this.wordList);
		tmp.removeAll(wordList.keySet());
		for (String s : tmp) {
			wordList.put(s, 0);
		}
		for (String word : wordList.keySet()) {
			sb.append("(");
			sb.append("\'" + word + "\'");
			sb.append(",");
			sb.append((wordList.get(word) * 1.0) / derCount);
			sb.append(",");
			sb.append("\'" + date + "\'");
			sb.append(")");
			if (i == wordList.keySet().size() - 1)
				break;
			else
				sb.append(",");
			i++;
			// Word is ast word in list finish query
		}
		return sb.toString();
	}

	/**
	 * + Adds triple to triplelist and returns triple strings (word,freq,date)
	 * 
	 * @param wordList
	 * @param date
	 * @param derCount
	 * @return
	 */
	public List<String> generateInsertTriples(
			HashMap<String, Integer> wordList, String date, int derCount) {
		List<String> result = new ArrayList<>();
		int i = 0;
		Set<String> tmp = new HashSet<String>(this.wordList);
		tmp.removeAll(wordList.keySet());
		for (String word : wordList.keySet()) {
			StringBuilder sb = new StringBuilder();
			sb.append("(");
			sb.append("\'" + word + "\'");
			sb.append(",");
			if (derCount != 0)
				sb.append((wordList.get(word) * 1.0) / derCount);
			sb.append(",");
			sb.append("\'" + date + "\'");
			sb.append(")");
			result.add(sb.toString());
			i++;
			getTripleList()
					.add(new Triple<String, Double, String>(
							word,
							derCount != 0 ? ((wordList.get(word) * 1.0) / derCount)
									: (0), date));
		}
		return result;
	}

	public String generateInsertQueryFromTripleList(List<String> tripleList) {
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT IGNORE INTO wfneu values");
		for (int i = 0; i < tripleList.size(); i++) {
			sb.append(tripleList.get(i));
			if (i != tripleList.size() - 1)
				sb.append(",");
			// Word is ast word in list finish query
		}
		return sb.toString();
	}

	public ArrayList<String> createWordList(ArrayList<String> dbNames,
			String query) {
		ArrayList<String> result = null;
		Set<String> wordSet = new HashSet<>();
		int i = 0;
		double startTime = new Date().getTime() / 1000.0;
		double endTime;
		for (String dbName : dbNames) {
			setDatabase(dbName);
			try {
				Statement stmt = getDatabaseConnection().createStatement();
				ResultSet rs = stmt.executeQuery(query);
				String wort;
				while (rs.next()) {
					wort = rs.getString(1).replace("\'", "\\\'");
					wordSet.add(wort);
				}
				stmt.close();
				System.out.println(++i + " / " + dbNames.size());
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		endTime = new Date().getTime() / 1000.0 - startTime;
		try {
			BufferedWriter bw = new BufferedWriter(
					new FileWriter("wordListLog"));
			bw.write("Words: " + wordSet.size());
			bw.newLine();
			bw.write("Databases: " + dbNames.size());
			bw.newLine();
			bw.write("Time in seconds: " + endTime);
			bw.write("Time(sec) per db: " + endTime / dbNames.size());
			bw.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println("Words in set: " + wordSet.size());
		result = new ArrayList<>(wordSet);
		return result;
	}

	public void executePreparedStatement(
			java.sql.PreparedStatement preparedStatement,
			ArrayList<Triple<String, Double, String>> tripleList) {
		try {
			int rowCount;
			long startTime;
			double timeNeeded;
			preparedStatement.getConnection().setAutoCommit(false);
			for (int i = 0; i < tripleList.size(); i++) {
				Triple<String, Double, String> triple = tripleList.get(i);
				preparedStatement.setString(1, triple.getOne());
				preparedStatement.setDouble(2, triple.getTwo());
				preparedStatement.setString(3, triple.getThree());
				preparedStatement.addBatch();
				if (i % 1000 == 0) {
					startTime = new Date().getTime();
					int[] count = preparedStatement.executeBatch();
					timeNeeded = ((new Date().getTime() - startTime) * 1.0) / 1000;
					System.out.println("Rows updated:" + count.length);
					System.out.println("took " + timeNeeded + "seconds");
					System.out.println("time to finish:"
							+ ((timeNeeded * (tripleList.size() - i)) / 1000));
				}
			}
			preparedStatement.executeBatch();
			preparedStatement.getConnection().commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Connection getDatabaseConnection() {
		return databaseConnection;
	}

	public void setDatabaseConnection(Connection databaseConnection) {
		this.databaseConnection = databaseConnection;
	}

	public PreparedStatement getpStmt() {
		return pStmt;
	}

	public void setpStmt(PreparedStatement pStmt) {
		this.pStmt = pStmt;
	}

	/**
	 * Start-Query for wordList select wort_bin from wortliste WHERE
	 * (SUBSTRING(wort_bin, 1, 1) COLLATE latin1_bin) =
	 * UPPER(SUBSTRING(wort_bin, 1, 1)) AND (SUBSTRING(wort_bin, 2,1) collate
	 * latin1_bin)=LOWER(SUBSTRING(wort_bin,2,1)) and length(wort_bin)>6 order
	 * by anzahl desc limit 1000;
	 */

	public ArrayList<Triple<String, Double, String>> getTripleList() {
		return tripleList;
	}

	public void setTripleList(
			ArrayList<Triple<String, Double, String>> tripleList) {
		this.tripleList = tripleList;
	}

	public void tripleListToFile(
			ArrayList<Triple<String, Double, String>> tripleList) {
		try {
			PrintWriter writer = new PrintWriter("tripleList.txt",
					Charsets.UTF_8.name());
			for (Triple triple : tripleList) {
				writer.println(triple.toString());
			}
			System.out.println("File written(lines):" + tripleList.size());
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public ArrayList<Triple<String, Double, String>> fileToTripleList(
			String fileName) {
		try {
			ArrayList<Triple<String, Double, String>> tripleList = new ArrayList<>();
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] vars = line.split(";");
				tripleList.add(new Triple<String, Double, String>(vars[0],
						Double.parseDouble(vars[1]), vars[2]));
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tripleList;
	}

	public void wordListToFile(List<String> wordList) {
		try {
			PrintWriter writer = new PrintWriter("wordList.txt",
					StandardCharsets.ISO_8859_1.name());
			for (String word : wordList) {
				writer.println(word);
			}
			System.out.println("File written(lines):" + tripleList.size());
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public List<String> fileToWordList(String fileName) {
		List<String> result = new ArrayList<>();
		int lineCounter = 0;

		try {
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			String line;
			while ((line = reader.readLine()) != null) {
				result.add(line);
				lineCounter++;
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Words loaded: " + lineCounter);
		return result;
	}

	public List<String> generateInsertQueryTripleList(boolean loadFile,
			String selectFreqQuery) {
		Queue<Long> window = new LinkedList<Long>();
		int period = 5;
		long sum = 0;
		Date time;
		int i = 1;
		Double start = new Date().getTime() / 1000.0;
		List<String> insertQueryTripleList = new ArrayList<String>();
		HashMap<String, Integer> result;
		if (!loadFile) {
			for (String db : dataBaseList) {
				time = new Date();
				// CONNECT TO DATABASE
				setDatabase(db);
				System.out.println(i + "/" + dataBaseList.size());
				System.out.print(db + " : ");
				// GET RELEVANT DATA
				int derCount = getAllWordsCount();
				String date = generateDateStringFromDBName(db);
				System.out.println(date);
				// GENERATE SELECT QUERY
				// Execute SelectQUERY
				result = executeSelectQuery(selectFreqQuery);
				insertQueryTripleList.addAll(generateInsertTriples(result,
						date, derCount));
				// TIME THING
				Long nTime = new Date().getTime() - time.getTime();
				sum += nTime;
				window.add(nTime);
				if (window.size() > period) {
					sum -= window.remove();
				}

				System.out.println(((sum / window.size()) * 1.0 / 1000)
						* (dataBaseList.size() - i));
				i++;
				// WRITE triplelist to file, triplelist is generated by
				// gernerateInsertTriples(...)
			}
			try {
				Double endTime = new Date().getTime() / 1000.0 - start;
				BufferedWriter bw = new BufferedWriter(new FileWriter(
						"generateinsertQLog"));
				bw.write("Triples: " + getTripleList().size());
				bw.newLine();
				bw.write("Databases: " + dataBaseList.size());
				bw.newLine();
				bw.write("Time in seconds: " + endTime);
				bw.write("Time(sec) per db: " + endTime / dataBaseList.size());
				bw.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} else {
			setTripleList(fileToTripleList("tripleList.txt"));
		}
		tripleListToFile(getTripleList());

		return insertQueryTripleList;
	}

	public List<String> generateInsertQueryTripleList(boolean loadFile,
			String selectFreqQuery, ArrayList<String> dataBaseList) {
		Queue<Long> window = new LinkedList<Long>();
		int period = 5;
		long sum = 0;
		Date time;
		int i = 1;
		List<String> insertQueryTripleList = new ArrayList<String>();
		HashMap<String, Integer> result;
		if (!loadFile)
			for (String db : dataBaseList) {
				time = new Date();
				// CONNECT TO DATABASE
				setDatabase(db);
				System.out.println(i + "/" + dataBaseList.size());
				System.out.print(db + " : ");
				// GET RELEVANT DATA
				int derCount = getDerCount();
				String date = generateDateStringFromDBName(db);
				System.out.println(date);
				// GENERATE SELECT QUERY
				// Execute SelectQUERY
				result = executeSelectQuery(selectFreqQuery);
				insertQueryTripleList.addAll(generateInsertTriples(result,
						date, derCount));
				// TIME THING
				Long nTime = new Date().getTime() - time.getTime();
				sum += nTime;
				window.add(nTime);
				if (window.size() > period) {
					sum -= window.remove();
				}
				System.out.println(((sum / window.size()) * 1.0 / 1000)
						* (dataBaseList.size() - i));

				i++;
				// WRITE triplelist to file, triplelist is generated by
				// gernerateInsertTriples(...)
			}
		else {
			setTripleList(fileToTripleList("tripleList.txt"));
		}
		tripleListToFile(getTripleList());

		return insertQueryTripleList;
	}

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// createRanksData();
		// createCorrellationDate();

		// derTest();
		// readTestCSV();

		mainSelect();

		// DataImporterImpl dii = new DataImporterImpl();
		// dii.connctToSQL();
		// String lastDate = dii.getLastDate();
		// updateTopWords();
		// updateTimeLineData();
		// updateDatesData(dii, lastDate);
		// updateRanksData();

	}

	private static void updateTopWords() {
		// TODO Auto-generated method stub

	}

	private static void updateRanksData() {
		// TODO Auto-generated method stub

	}

	private static void updateDatesData(DataImporterImpl dii, String lastDate) {
		ArrayList<String> dateList = dii.getWDTDatabases();
		SimpleDateFormat dtf = new SimpleDateFormat("yyyy-MM-dd");
		ArrayList<String> newDatesList = new ArrayList<>();
		for (String dateStr : dateList) {
			try {
				if (dtf.parse(dii.generateDateStringFromDBName(dateStr))
						.compareTo(dtf.parse(lastDate)) > 0) {
					System.out.println(dii
							.generateDateStringFromDBName(dateStr));
					newDatesList.add(dateStr);
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		dii.setDatabase("mkis_bachelor");
		try {
			PreparedStatement pstmt = dii.getDatabaseConnection()
					.prepareStatement("insert into dates values(?)");
			for (String date : newDatesList) {
				pstmt.setString(1, dii.generateDateStringFromDBName(date));
				pstmt.addBatch();
			}
			System.out.println(pstmt.executeBatch().length + " dates added");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void updateTimeLineData() {
		DataImporterImpl dii = new DataImporterImpl();
		dii.connctToSQL();
		ArrayList<String> dateList = dii.getWDTDatabases();
		SimpleDateFormat dtf = new SimpleDateFormat("yyyy-MM-dd");
		String lastDate = dii.getLastDate();
		System.out.println("last date: " + lastDate);
		ArrayList<String> newDatesList = new ArrayList<>();
		dii.setWordList(dii.getServerWordList());
		for (String dateStr : dateList) {
			try {
				if (dtf.parse(dii.generateDateStringFromDBName(dateStr))
						.compareTo(dtf.parse(lastDate)) > 0) {
					System.out.println(dii
							.generateDateStringFromDBName(dateStr));
					newDatesList.add(dateStr);
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		dii.generateInsertQueryTripleList(false,
				dii.generateSelectQuery(dii.getWordList()), newDatesList);
		try {
			dii.setDatabase("mkis_bachelor");
			Statement stmt = dii.getDatabaseConnection().createStatement();
			stmt.executeUpdate("load data local infile \'triplelist.txt\' "
					+ "into table frequencies "
					+ "fields terminated by \';\' lines terminated by \'\\n\' "
					+ "(word,frequency,date)");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		dii.close();
	}

	public String getLastDate() {
		String res = "";
		setDatabase("mkis_bachelor");
		Date lastDate;
		SimpleDateFormat dtf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Statement stmt = getDatabaseConnection().createStatement();
			ResultSet rs = stmt
					.executeQuery("select date from dates order by date desc limit 1");
			rs.next();
			lastDate = rs.getDate(1);
			res = dtf.format(lastDate);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return res;
	}

	private static void createCorrellationDate() {
		DataImporterImpl dii = new DataImporterImpl();
		dii.connctToSQL();
		dii.setDatabase("mkis_bachelor");
		List<String> wordList = dii.getServerWordList();
		List<Date> dateList = dii.getServerDates();
		System.out.println("Words loaded: " + wordList.size());
		TreeMap<Date, WordAtDateData> mapOne;
		TreeMap<Date, WordAtDateData> mapTwo;
		String updateQuery = "UPDATE frequencies set rank=? where word=? and date=?";
		int i = 1;
		int rowsUpdated = 0;
		try {
			dii.getDatabaseConnection().setAutoCommit(false);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		long restTime = 0;
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Set<String> allWords = new HashSet(wordList);
		Set<String> doneWords = new HashSet();
		ArrayList<Triple<String, String, Double>> tripleList = new ArrayList<>();
		HashMap<String, TreeMap<Date, WordAtDateData>> timeLines = new HashMap<>();
		double[] x = new double[dateList.size()];
		double[] y = new double[dateList.size()];
		boolean firstRun = true;
		Double pearson;
		int c = 0;
		for (String wordOne : wordList) {
			Date start = new Date();
			if (firstRun) {
				mapOne = dii.getTimeLine(wordOne);
				timeLines.put(wordOne, mapOne);
			} else {
				mapOne = timeLines.get(wordOne);
			}
			c = 0;
			for (Date date : dateList) {
				if (mapOne.get(date) == null) {
					x[c] = 0;
				} else {
					x[c] = (double) mapOne.get(date).getFreq();
				}
				c++;
			}
			doneWords.add(wordOne);
			allWords.removeAll(doneWords);
			for (String wordTwo : allWords) {
				if (firstRun) {
					mapTwo = dii.getTimeLine(wordTwo);
					timeLines.put(wordTwo, mapTwo);
				} else {
					mapTwo = timeLines.get(wordTwo);
				}
				c = 0;
				for (Date date : dateList) {
					if (mapTwo.get(date) == null) {
						y[c] = 0;
					} else {
						y[c] = (double) mapTwo.get(date).getFreq();
					}
					c++;
				}
				pearson = new PearsonsCorrelation().correlation(x, y);
				tripleList.add(new Triple<String, String, Double>(wordOne,
						wordTwo, pearson));
			}
			// TIME
			restTime = (new Date().getTime() - start.getTime()) / 1000
					* (wordList.size() - i);
			System.out.println("Committed " + i + "/" + wordList.size()
					+ " Rest time : " + restTime);
			i++;
		}
		createCSVFromTripleList(tripleList, "correlations", false);
		try {
			// pstmt.close();
			dii.getDatabaseConnection().close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void peakSearch() {
		String dbHost = "woclu3.informatik.uni-leipzig.de";
		String dbPort = "3306";
		String newDBName = "mkis_bachelor";
		String dbUsername = "mkisilowski";
		String dbPassword = "D,hdn1-Rd?";
		Connection dbCon;
		String url = "jdbc:mysql://" + dbHost + ":" + dbPort;
		try {
			TreeMap<Date, Float> result = new TreeMap<>();
			Class.forName("com.mysql.jdbc.Driver");
			dbCon = DriverManager.getConnection(url, dbUsername, dbPassword);
			System.out.println("SQL-Connection established");
			dbCon.setCatalog(newDBName);
			Statement stmt = dbCon.createStatement();
			ResultSet rs = stmt
					.executeQuery("select datum,frequenz from wfneu where wort='Barcelona' order by datum asc");
			while (rs.next()) {
				result.put(rs.getDate(1), rs.getFloat(2));
			}
			System.out.println(result.size());
			rs.close();
			stmt.close();
			dbCon.close();

			DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd");
			Float old = 0F;
			Float now = 0F;
			float average = 0F;
			Date[] dates = new Date[result.size()];
			result.keySet().toArray(dates);
			for (int i = 0; i < dates.length; i++) {
				LocalDate ld = LocalDate.fromDateFields(dates[i]);
				now = result.get(dates[i]);
				if (now > old) {
					old = now;
					System.out.println(ld.toString(dtf) + " : " + now);
				}
				average += now;
			}
			average = average / result.size();
			int oldWeek = LocalDate.fromDateFields(dates[0])
					.getWeekOfWeekyear();
			int oldMonth = LocalDate.fromDateFields(dates[0]).getMonthOfYear();
			float monthlyAverage = 0F;
			float weeklyAverage = 0F;
			boolean inCalcWeeklAvg = true;
			boolean inCalcMonthAvg = true;
			int dayc = 0;
			int peakCount = 0;
			int weekPeakCount = 0;
			for (int i = 0; i < dates.length; i++) {
				LocalDate ld = LocalDate.fromDateFields(dates[i]);
				if (inCalcWeeklAvg) {
					if (ld.getMonthOfYear() != oldMonth) {
						weeklyAverage = weeklyAverage / dayc;
						if (weeklyAverage > average) {
							System.out.println("wAverage : " + oldWeek + ":"
									+ weeklyAverage);
							i = i - dayc + 1;
							inCalcWeeklAvg = false;
							weekPeakCount++;
						} else {
							dayc = 0;
						}
					}
					dayc++;
					weeklyAverage += result.get(dates[i]);
				} else {
					if (result.get(dates[i]) > weeklyAverage) {
						System.out.println("wekl: " + weeklyAverage + " day: "
								+ ld.toString(dtf) + " freq: "
								+ result.get(dates[i]));
						peakCount++;
					}
					if (ld.getMonthOfYear() != oldMonth) {
						oldMonth = ld.getMonthOfYear();
						dayc = 0;
						weeklyAverage = 0F;
						inCalcWeeklAvg = true;
					}
				}

			}
			System.out.println("Peak Count: " + peakCount);
			System.out.println("weakpeak" + weekPeakCount);
			System.out.println("Average : " + average);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void mainSelect() {
		String wlQuery = "select wort_alph,anzahl from wortliste"
				+ " where length(wort_bin) > 1 and "
				+ "SUBSTRING(wort_bin, 1, 1) regexp '[[:alpha:]]' "
				+ "order by anzahl desc limit 1000";
		// List<String> wordList = Arrays.asList(wordArray);
		DataImporterImpl dii = new DataImporterImpl();
		dii.connctToSQL();
		ArrayList<String> dataBaseList = dii.getWDTDatabases();
		// List<String> wordList = dii.createWordList(dataBaseList, wlQuery);
		// dii.wordListToFile(wordList);
		List<String> wordList = dii.fileToWordList("wordList.txt");
		dii.setWordList(wordList);
		dii.generateInsertQueryTripleList(false,
				dii.generateSelectQuery(wordList));
		// dii.wordListToFile(wordList);
		System.out.println("ENDE");
	}

	public static void readTestCSV() {
		CSVParser csvp;
		DescriptiveStatistics ds = new DescriptiveStatistics();
		try {
			csvp = new CSVParser(new FileReader("dertest1.txt"),
					CSVFormat.DEFAULT);
			double tmp;
			for (CSVRecord rec : csvp.getRecords()) {
				// System.out.println(rec.get(3) + "");
				tmp = Double.parseDouble(rec.get(3));
				if (tmp != Double.NaN && tmp != 0)
					ds.addValue(tmp);

			}
			csvp.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("Results:");
		System.out.println(ds.getMax());
		System.out.println(ds.getMin());
		System.out.println(ds.getMean());
	}

	public static void derTest() {
		DataImporterImpl dii = new DataImporterImpl();
		dii.connctToSQL();
		ArrayList<String> dataBaseList = dii.getWDTDatabases();
		ArrayList<String[]> result = new ArrayList<String[]>();
		List<String> dataRowNew = new ArrayList<>();

		int derSum = 0;
		int allSum = 0;

		try {
			int i = 0;
			CSVPrinter csvp = new CSVPrinter(new PrintWriter("dertest.txt"),
					CSVFormat.DEFAULT);
			for (String db : dataBaseList) {
				dii.setDatabase(db);
				dataRowNew.add(dii.generateDateStringFromDBName(db));
				int derCount = dii.getDerCount();
				int allCount = dii.getAllWordsCount();
				dataRowNew.add(derCount + "");
				derSum += derCount;
				dataRowNew.add(allCount + "");
				allSum += allCount;
				dataRowNew.add("" + (derCount * 1.0) / allCount);
				result.add(dataRowNew.toArray(new String[dataRowNew.size()]));
				dataRowNew.clear();
				i++;
				System.out.println(i + " /" + dataBaseList.size());

			}
			dataRowNew.add("0000-00-00");
			dataRowNew.add("" + (derSum * 1.0) / result.size());
			dataRowNew.add("" + (allSum * 1.0) / result.size());
			dataRowNew.add("" + (derSum * 1.0) / allSum);
			result.add(dataRowNew.toArray(new String[dataRowNew.size()]));

			csvp.printRecords(result);
			csvp.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void createRanksData() {
		DataImporterImpl dii = new DataImporterImpl();
		dii.connctToSQL();
		dii.setDatabase("mkis_bachelor");
		List<String> wordList = dii.getServerWordList();
		System.out.println("Words loaded: " + wordList.size());
		TreeMap<Date, WordAtDateData> map;
		String updateQuery = "UPDATE frequencies set rank=? where word=? and date=?";
		int i = 1;
		int rowsUpdated = 0;
		try {
			dii.getDatabaseConnection().setAutoCommit(false);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		long restTime = 0;
		ArrayList<Triple<byte[], Date, Integer>> tripleList = new ArrayList<>();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		for (String word : wordList) {
			Date start = new Date();
			map = dii.getTimeLine(word);
			try {
				for (Date date : map.keySet()) {
					if (map.get(date).getRank() >= 5) {
						tripleList.add(new Triple<byte[], Date, Integer>(word
								.getBytes(), date, new Integer(map.get(date)
								.getRank())));
					}
				}
				restTime = (new Date().getTime() - start.getTime()) / 1000
						* (wordList.size() - i);
				System.out.println("Committed " + i + "/" + wordList.size()
						+ " Rest time : " + restTime);
				i++;
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(updateQuery);
				for (byte b : word.getBytes()) {
					System.out.print(b + " ");
				}
			}
		}
		try {
			// pstmt.close();
			dii.getDatabaseConnection().close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		createCSVFromTripleList(tripleList, "ranks.txt");

	}

	private static void createCSVFromTripleList(
			ArrayList<Triple<String, String, Double>> tripleList,
			String fileName, boolean ba) {
		try {
			PrintWriter writer = new PrintWriter(fileName,
					StandardCharsets.UTF_8.name());
			for (Triple<String, String, Double> triple : tripleList) {
				writer.println(new String(triple.getOne()) + ";"
						+ triple.getTwo() + ";" + triple.getThree());
			}
			System.out.println("File written(lines):" + tripleList.size());
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void createCSVFromTripleList(
			ArrayList<Triple<byte[], Date, Integer>> tripleList, String fileName) {
		try {
			PrintWriter writer = new PrintWriter(fileName,
					StandardCharsets.UTF_8.name());
			for (Triple<byte[], Date, Integer> triple : tripleList) {
				StringBuilder bString = new StringBuilder();
				for (byte b : triple.getOne()) {
					bString.append(b);
				}
				writer.println(new String(triple.getOne()) + ";"
						+ triple.getTwo() + ";" + triple.getThree());
			}
			System.out.println("File written(lines):" + tripleList.size());
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private String createUpdateQuery(TreeMap<Date, WordAtDateData> map,
			String word) {
		StringBuilder sb = new StringBuilder();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		sb.append("UPDATE frequencies set peak=1 where word = '" + word
				+ "' and date in (");
		for (Date date : map.keySet()) {
			if (map.get(date).isPeak()) {
				sb.append("'" + dateFormat.format(date) + "',");
			}
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append(")");
		return sb.toString();
	}

	public TreeMap<Date, WordAtDateData> getTimeLine(String word) {
		PreparedStatement pstmt;
		String freqTableName = "frequencies";
		String query = "select frequency,date from " + freqTableName
				+ " where word = ? order by date desc";
		ResultSet rs;
		TreeMap<Date, WordAtDateData> result = new TreeMap<>();
		try {
			pstmt = getDatabaseConnection().prepareStatement(query);
			pstmt.setBytes(1, word.getBytes());
			rs = pstmt.executeQuery();
			while (rs.next()) {
				result.put(rs.getDate(2), new WordAtDateData(rs.getFloat(1)));
			}
			return addRanksToTimeLine(result);
		} catch (SQLException e) {
			e.printStackTrace();
			return result;
		}
	}

	private TreeMap<Date, WordAtDateData> addPeaksToTimeLineML(
			TreeMap<Date, WordAtDateData> data) {
		TreeMap<Date, WordAtDateData> result = data;
		List<WordWrapper> clusterInput = new ArrayList<WordWrapper>();
		double x = 0;
		double gYM = 0;
		double tmp = 0;
		DescriptiveStatistics stats = new DescriptiveStatistics();
		int c = 0;
		for (WordAtDateData wadd : data.values()) {
			c++;
			tmp += wadd.getFreq();
			if (c == 90) {
				tmp = tmp / c;
				if (tmp > gYM)
					gYM = tmp;
				tmp = 0;
				c = 0;
			}
			stats.addValue(wadd.getFreq());
		}
		tmp = tmp / c;
		if (tmp > gYM)
			gYM = tmp;
		double median = gYM;
		double mean = stats.getMean();
		for (Date date : data.keySet()) {
			data.get(date).setDateLong(date.getTime());
			clusterInput.add(new WordWrapper(data.get(date), x));
			x++;
		}
		if (clusterInput.size() > 0) {
			int clusterCount = data.size() / 10 + 1;
			KMeansPlusPlusClusterer<WordWrapper> clusterer = new KMeansPlusPlusClusterer<>(
					clusterCount, 10000);
			MultiKMeansPlusPlusClusterer<WordWrapper> multiclusterer = new MultiKMeansPlusPlusClusterer<>(
					clusterer, 5);
			List<CentroidCluster<WordWrapper>> clusterResults = multiclusterer
					.cluster(clusterInput);
			for (int i = 0; i < clusterResults.size(); i++) {
				if (median < clusterResults.get(i).getCenter().getPoint()[1]
						&& mean < clusterResults.get(i).getCenter().getPoint()[1]) {
					for (WordWrapper wordWrapper : clusterResults.get(i)
							.getPoints()) {
						if (median < wordWrapper.getWord().getFreq()) {
							result.get(
									new Date(wordWrapper.getWord()
											.getDateLong())).setPeak(true);
							result.get(
									new Date(wordWrapper.getWord()
											.getDateLong())).setPeakComment(
									"Cluster" + i);
						}

					}
				}
			}
		}
		return result;
	}

	private TreeMap<Date, WordAtDateData> addRanksToTimeLine(
			TreeMap<Date, WordAtDateData> data) {
		TreeMap<Date, WordAtDateData> result = new TreeMap<>();
		DescriptiveStatistics mainstat = new DescriptiveStatistics();
		for (Date date : data.keySet()) {
			result.put(
					date,
					new WordAtDateData(data.get(date).getFreq(), date.getTime()));
			mainstat.addValue((double) data.get(date).getFreq());
		}
		double maxVal = mainstat.getMax();
		mainstat.clear();
		for (Date date : data.keySet()) {
			data.get(date).setFreq((float) (data.get(date).getFreq() / maxVal));
			mainstat.addValue((double) data.get(date).getFreq());
		}
		mainstat.clear();
		mainstat.setWindowSize(364);
		for (Date date : data.keySet()) {
			float diff;
			if (mainstat.getMean() == 0) {
				diff = 0F;
			} else {
				diff = (float) data.get(date).getFreq()
						/ (float) mainstat.getMean();
				// * (float) mainstat.getVariance();
			}
			result.get(date).setRank((int) Math.rint(diff));
			mainstat.addValue(data.get(date).getFreq());
		}
		return result;
	}

	private List<Date> getServerDates() {
		String query = "Select date from dates order by date desc";
		ArrayList<Date> result = new ArrayList<>();
		try {
			Statement stmt = getDatabaseConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				result.add(rs.getDate(1));
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;

	}

	public List<String> getServerWordList() {
		String query = "Select distinct word from words";
		ArrayList<String> result = new ArrayList<>();
		try {
			Statement stmt = getDatabaseConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
			byte[] word;
			while (rs.next()) {
				word = (byte[]) rs.getObject(1);
				result.add(new String(word));
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}
}
