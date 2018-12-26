package ass4db;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Calendar;
import java.util.Scanner;


import java.text.SimpleDateFormat;
import java.io.File;
import java.io.FileNotFoundException;

import javafx.util.Pair;

import java.util.ArrayList;



public class Assignment4 {


	//for the regular workflow of the program besides the init method
	private static String JDBC_CONNECTION_URL="jdbc:sqlserver://localhost;username=LAPTOP-Q6DKH3TT/SQLEXPRESS;databaseName=DB2019_Ass2;integratedSecurity=true;";
	//for the init db method
	private static String JDBC_CONNECTION_URL_INIT="jdbc:sqlserver://localhost;username=LAPTOP-Q6DKH3TT/SQLEXPRESS;integratedSecurity=true;";


	private Assignment4() {
	}

	public static void executeFunc(Assignment4 ass, String[] args) {
		String funcName = args[0];
		switch (funcName) {
		case "loadNeighborhoodsFromCsv":
			ass.loadNeighborhoodsFromCsv(args[1]);
			break;
		case "dropDB":
			ass.dropDB();
			break;
		case "initDB":
			ass.initDB(args[1]);
			break;
		case "updateEmployeeSalaries":
			ass.updateEmployeeSalaries(Double.parseDouble(args[1]));
			break;
		case "getEmployeeTotalSalary":
			System.out.println(ass.getEmployeeTotalSalary());
			break;
		case "updateAllProjectsBudget":
			ass.updateAllProjectsBudget(Double.parseDouble(args[1]));
			break;
		case "getTotalProjectBudget":
			System.out.println(ass.getTotalProjectBudget());
			break;
		case "calculateIncomeFromParking":
			System.out.println(ass.calculateIncomeFromParking(Integer.parseInt(args[1])));
			break;
		case "getMostProfitableParkingAreas":
			System.out.println(ass.getMostProfitableParkingAreas());
			break;
		case "getNumberOfParkingByArea":
			System.out.println(ass.getNumberOfParkingByArea());
			break;
		case "getNumberOfDistinctCarsByArea":
			System.out.println(ass.getNumberOfDistinctCarsByArea());
			break;
		case "AddEmployee":
			SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy");
			ass.AddEmployee(Integer.parseInt(args[1]), args[2], args[3], java.sql.Date.valueOf(args[4]), args[5], Integer.parseInt(args[6]), Integer.parseInt(args[7]), args[8]);
			break;
		default:
			break;
		}
	}



	public static void main(String[] args) {

		File file = new File(".");
		String csvFile = args[0];
		String line = "";
		String cvsSplitBy = ",";
		Assignment4 ass = new Assignment4();
		try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

			while ((line = br.readLine()) != null) {

				// use comma as separator
				String[] row = line.split(cvsSplitBy);

				executeFunc(ass, row);

			}

		} catch (IOException e) {
			e.printStackTrace();

		} 
	}



	private void loadNeighborhoodsFromCsv(String csvPath) {
		Connection con=getCon();	//get the connection with DB
		try {
			Statement st =con.createStatement();
			int results;	//get the feedback from query

			File file=new File(csvPath);
			try{
				Scanner inputStream=new Scanner(file);
				while(inputStream.hasNext()){			//read csv file data
					String data=inputStream.next();
					String[] fields=data.split(",");	//split by commas

					int nid=Integer.parseInt(fields[0]);	//get the NID from file
					String name=fields[1];						//get the Name from file
					//insert a row to the neighborhood table
					results=st.executeUpdate("INSERT INTO Neighborhood (NID,Name) VALUES('"+nid+"','"+name+"')");
				}
				//close stream
				inputStream.close();

			}catch(FileNotFoundException e){
				e.printStackTrace();
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//close connection
		if(con!=null){
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
	/**
	 * update constructor employees salary
	 * @param percentage - update salary by this param
	 */
	private void updateEmployeeSalaries(double percentage) {		

		Connection con=getCon();	//open connection
		try {
			Statement st=con.createStatement();		//create statement
			PreparedStatement updateSalaryStatement=null;
			//get the constructor employees data of those who are 50 years old or older
			String selectQuery="SELECT ce.EID,SalaryPerDay FROM ConstructorEmployee ce,Employee e WHERE ce.EID=e.EID AND DATEDIFF(year,e.BirthDate,GETDATE())>49;";
			String updateSalaryString="UPDATE ConstructorEmployee SET SalaryPerDay = ? WHERE EID = ?";
			ResultSet results=st.executeQuery(selectQuery);		//get constructor employees data
			//prepare update statement
			try{
				con.setAutoCommit(false);
				updateSalaryStatement=con.prepareStatement(updateSalaryString);

				while(results.next()){	//get the salary of each employee and update by raise percentage
					int eid=results.getInt(1);
					int oldSalary=results.getInt(2);
					//calculate new salary after raise
					int raisedSalary=(int)(oldSalary*(percentage/100)+oldSalary);	//check if the casting is correct!

					//update the new salary in the db table of constructor employees in age 50 and more
					updateSalaryStatement.setInt(1, raisedSalary);
					updateSalaryStatement.setInt(2, eid);
					updateSalaryStatement.executeUpdate();
					con.commit();

				}

			}catch(SQLException e){
				e.printStackTrace();
				if(con!=null){
					try{
						System.err.print("Transaction is being rolled back");
						con.rollback();
					}catch(SQLException error){
						error.printStackTrace();
					}
				}
			}

		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		if(con!=null){	//close connection with db
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}


	public void updateAllProjectsBudget(double percentage) {

		Connection con=getCon();	//open connection
		try {
			Statement st=con.createStatement();		//create statement
			PreparedStatement updateBudgetStatement=null;
			//get the projects data
			String selectQuery="SELECT PID,Budget FROM Project";
			String updateBudgetString="UPDATE Project SET Budget = ? WHERE PID = ?";
			ResultSet results=st.executeQuery(selectQuery);		//get projects data
			//prepare update statement
			try{
				con.setAutoCommit(false);
				updateBudgetStatement=con.prepareStatement(updateBudgetString);

				while(results.next()){	//get the budget of each project and update by raise percentage
					int pid=results.getInt(1);
					int oldBudget=results.getInt(2);
					//calculate new budget after raise
					int raisedBudget=(int)(oldBudget*(percentage/100)+oldBudget);	//check if the casting is correct!

					//update the budget
					updateBudgetStatement.setInt(1, raisedBudget);
					updateBudgetStatement.setInt(2, pid);
					updateBudgetStatement.executeUpdate();
					con.commit();
				}

			}catch(SQLException e){
				e.printStackTrace();
				if(con!=null){
					try{
						System.err.print("Transaction is being rolled back");
						con.rollback();
					}catch(SQLException error){
						error.printStackTrace();
					}
				}
			}

		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		if(con!=null){	//close connection with db
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}





	/**
	 * get the sum of cons. employees salaries
	 * @return @param total
	 */
	private double getEmployeeTotalSalary() {	

		double total=0;	//holds the sum of the constructor employee salaries

		Connection con=getCon();	//open connection
		try {
			Statement st=con.createStatement();		//create statement
			//get the salaryPerDay data
			String selectQuery="SELECT SalaryPerDay FROM ConstructorEmployee";
			ResultSet results=st.executeQuery(selectQuery);		//get constructor employees data
			while(results.next()){
				int iterSalary=results.getInt(1);
				total+=iterSalary;
			}

		}catch(SQLException e){
			e.printStackTrace();
		}

		if(con!=null){	//close connection with db
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return total;

	}

	/**
	 * get the total budget for whole projects
	 * @return total-total budget
	 */
	private int getTotalProjectBudget() {

		int total=0;	//holds the sum of the budgets

		Connection con=getCon();	//open connection
		try {
			Statement st=con.createStatement();		//create statement
			//get the budget data
			String selectQuery="SELECT Budget FROM Project";
			ResultSet results=st.executeQuery(selectQuery);		//get budget per project data
			while(results.next()){
				int iterBudget=results.getInt(1);
				total+=iterBudget;
			}

		}catch(SQLException e){
			e.printStackTrace();
		}

		if(con!=null){	//close connection with db
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return total;

	}
	/**
	 * deleteDB DB2019_Ass2
	 */
	private void dropDB() {
		Connection con=getConWithMaster();
		String DBNameToDrop="temp";						//change to DB2019_Ass2 before submitting
		//String DBNameToDrop="DB2019_Ass2";
		String sqlString="DROP DATABASE "+DBNameToDrop;
		try {
			Statement st=con.createStatement();
			st.executeUpdate(sqlString);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(con!=null){
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	/**
	 * run ddl script which is get as @param
	 * @param csvPath -the path for the .sql file with dll commands
	 */
	private void initDB(String csvPath) {


		String readQuery="";
		Connection con=getConWithMaster();	//get the connection with DB
		try {
			Statement st =con.createStatement();
			long results;	//get the feedback from query

			File file=new File(csvPath);
			try{
				Scanner inputStream=new Scanner(file);
				while(inputStream.hasNextLine()){			//read csv file data
					String nextLine=inputStream.nextLine();
					if(!nextLine.equals("GO")&&(!strIsCreateDatabase(nextLine))){
						readQuery+=nextLine;
						readQuery+="\n";
					}
					else{	//create DB
						if(!nextLine.equals("GO")){
							results=st.executeUpdate(nextLine);
						}
					}
				}
				results=st.executeUpdate(readQuery);

				//close stream
				inputStream.close();

			}catch(FileNotFoundException e){
				e.printStackTrace();
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//close connection
		if(con!=null){
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}


	}
	/**
	 * calculate income of parking in received @param=year
	 * @param year
	 * @return totalIncome
	 */
	private int calculateIncomeFromParking(int year) {


		int totalIncome=0;	//holds the sum of the costs payed in 'year'

		Connection con=getCon();	//open connection
		try {
			Statement st=con.createStatement();		//create statement
			//get the cost data
			String selectQuery="SELECT Cost FROM CarParking WHERE YEAR(EndTime)='"+year+"'";
			ResultSet results=st.executeQuery(selectQuery);		//get budget per project data
			while(results.next()){
				int iterIncome=results.getInt(1);
				totalIncome+=iterIncome;
			}

		}catch(SQLException e){
			e.printStackTrace();
		}

		if(con!=null){	//close connection with db
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return totalIncome;

	}

	/**
	 * 
	 * @return array list of pairs <parkingAreaID,profitCalculated> of top 5 profit parking areas
	 */

	private ArrayList<Pair<Integer, Integer>> getMostProfitableParkingAreas() {
		ArrayList<Pair<Integer, Integer>> mostProfitPAreas=null;		//holds the top 5 profit parking areas

		Connection con=getCon();	//open connection
		try {
			Statement st=con.createStatement();		//create statement
			//get the cost data
			String selectQuery="SELECT TOP 5 ParkingAreaID, SUM(Cost) AS Profit FROM CarParking group by ParkingAreaID ORDER BY Profit DESC";
			ResultSet results=st.executeQuery(selectQuery);		//get profit per area data
			mostProfitPAreas=new ArrayList<>(5);
			while(results.next()){
				int parkingAreaID=results.getInt(1);
				int profit=results.getInt(2);
				mostProfitPAreas.add(new Pair(new Integer(parkingAreaID),new Integer(profit)));
			}

		}catch(SQLException e){
			e.printStackTrace();
		}

		if(con!=null){	//close connection with db
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}


		return mostProfitPAreas;
	}

	/**
	 * 
	 * @return array list of pairs <parkingAreaID,amount of parkings in this area> 
	 */
	private ArrayList<Pair<Integer, Integer>> getNumberOfParkingByArea() {

		ArrayList<Pair<Integer, Integer>> amountParkingsAtArea=null;		//holds parkings at parking areas

		Connection con=getCon();	//open connection
		try {
			Statement st=con.createStatement();		//create statement
			//get the cost data
			String selectQuery="SELECT ParkingAreaID, COUNT(*) AS parkingAmount FROM CarParking group by ParkingAreaID ORDER BY ParkingAreaID";
			ResultSet results=st.executeQuery(selectQuery);		//get amount of parkings in areas
			amountParkingsAtArea=new ArrayList<>();
			while(results.next()){
				int parkingAreaID=results.getInt(1);
				int amount=results.getInt(2);		//get amount of parkings in iterated area
				amountParkingsAtArea.add(new Pair(new Integer(parkingAreaID),new Integer(amount)));
			}

		}catch(SQLException e){
			e.printStackTrace();
		}

		if(con!=null){	//close connection with db
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return amountParkingsAtArea;

	}

	/**
	 * get the amount of cars in each parking area
	 * @return carsAtArea-key=parking area, value amount of distinct cars
	 */
	private ArrayList<Pair<Integer, Integer>> getNumberOfDistinctCarsByArea() {

		ArrayList<Pair<Integer, Integer>> carsAtArea=null;		

		Connection con=getCon();	//open connection
		try {
			Statement st=con.createStatement();		//create statement
			//get the cost data
			String selectQuery="SELECT ParkingAreaID, COUNT(DISTINCT CID) AS carsAmount FROM CarParking GROUP BY ParkingAreaID ORDER BY ParkingAreaID";
			ResultSet results=st.executeQuery(selectQuery);		//get amount of cars in areas
			carsAtArea=new ArrayList<>();
			while(results.next()){
				int parkingAreaID=results.getInt(1);
				int amount=results.getInt(2);		//get amount of parkings in iterated area
				carsAtArea.add(new Pair(new Integer(parkingAreaID),new Integer(amount)));
			}

		}catch(SQLException e){
			e.printStackTrace();
		}

		if(con!=null){	//close connection with db
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return carsAtArea;
	}

	/**
	 * add Employee with following details to the Employee table
	 * @param EID
	 * @param LastName
	 * @param FirstName
	 * @param BirthDate
	 * @param StreetName
	 * @param Number
	 * @param door
	 * @param City
	 */
	private void AddEmployee(int EID, String LastName, String FirstName, Date BirthDate, String StreetName, int Number, int door, String City) {

		Connection con=getCon();	//get the connection with DB
		try {
			Statement st =con.createStatement();

			//insert a row to the Employee table
			try {
				//results get the feedback from query
				int results=st.executeUpdate("INSERT INTO Employee (EID,LastName,FirstName,BirthDate,StreetName,Number,door,City) VALUES('"+EID+"','"+LastName+"','"+FirstName+"','"+BirthDate+"','"+StreetName+"','"+Number+"','"+door+"','"+City+"')");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}catch(SQLException ex){
			ex.printStackTrace();
		}

		//close connection
		if(con!=null){
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	/**
	 * the following returns a connection with sql db of assignment
	 */
	private static Connection getCon() {
		Connection connection = null;
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			connection = DriverManager.getConnection(JDBC_CONNECTION_URL);

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return connection;
	}



	/**
	 * the following returns a connection with sql master db	
	 */
	private static Connection getConWithMaster() {
		Connection connection = null;
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			connection = DriverManager.getConnection(JDBC_CONNECTION_URL_INIT);

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return connection;
	}
	/**
	 * check if the string represent a create DB command
	 * @param str
	 * @return true-create db command=str,otherwise false
	 */
	public static boolean strIsCreateDatabase(String str){
		boolean ans=false;

		if(str.startsWith("Create d")||str.startsWith("CREATE d")||str.startsWith("Create D")||str.startsWith("CREATE D")||
				str.startsWith("create d")||str.startsWith("create D")){
			ans=true;
		}

		return ans;
	}
}


