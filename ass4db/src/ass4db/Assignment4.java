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


	private static String JDBC_CONNECTION_URL="jdbc:sqlserver://localhost;username=LAPTOP-Q6DKH3TT/SQLEXPRESS;databaseName=DB2019_Ass2;integratedSecurity=true;";
	
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

    	
    	String csvExample="exampleCsv.csv";
        Assignment4 ass = new Assignment4();
        //ass.loadNeighborhoodsFromCsv(csvExample);	//test q1
        ass.updateEmployeeSalaries(100);            //test q2
    	System.out.println("session ended!!!!");
    	
    	
    	/*													//////	only the commented is the original code//////
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

        } */
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
	    			//insert a row to the neigborhood table
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
    private void updateEmployeeSalaries(double percentage) {		//ask if to update constructor as did or Employee

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
					
					System.out.println("eid: "+eid+" old salary: "+oldSalary+" new salary: "+raisedSalary);	//for tests only! remove after!
					
					//update the new salary in the db table of constructore employees in age 50 and more
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

    }


    private double getEmployeeTotalSalary() {
		return 0;

    }


    private int getTotalProjectBudget() {
		return 0;

    }
    private void dropDB() {

    }

    private void initDB(String csvPath) {

    }
    private int calculateIncomeFromParking(int year) {
		return 0;

    }

    private ArrayList<Pair<Integer, Integer>> getMostProfitableParkingAreas() {
		return null;

    }

    private ArrayList<Pair<Integer, Integer>> getNumberOfParkingByArea() {
		return null;

    }


    private ArrayList<Pair<Integer, Integer>> getNumberOfDistinctCarsByArea() {
		return null;

    }


    private void AddEmployee(int EID, String LastName, String FirstName, Date BirthDate, String StreetName, int Number, int door, String City) {

    }
    
    /**
     * the following returns a connection with sql db of assignment	------verify right name of db!!!!!!!!!!!!!
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
}
    
    
    

