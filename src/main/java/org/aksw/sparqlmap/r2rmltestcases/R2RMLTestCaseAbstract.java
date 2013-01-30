package org.aksw.sparqlmap.r2rmltestcases;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.openjena.riot.RiotLoader;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.DatasetGraph;

public abstract class R2RMLTestCaseAbstract {
	
	
	/**
	 * returns a brand new Database connection which must be closed afterwards
	 * @return
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 */
	public abstract Connection getConnection() throws ClassNotFoundException, SQLException;
	
	
	/**
	 * creates the properties to put into the spring container.
	 * @return
	 */
	public abstract Properties getDBProperties();
	
	/**
	 * closes the connection
	 * @param conn
	 */
	public void closeConnection(Connection conn){
		//crappy connection handling is ok here.
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	
	/**
	 * load the file into the database
	 * @param file
	 * @return
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 */
	public void loadFileIntoDB(String file) throws ClassNotFoundException, SQLException, IOException{
		
		
		String sql2Execute = FileUtils.readFileToString(new File(file));
		loadStringIntoDb(sql2Execute);
		
		
	}


	public void loadStringIntoDb(String sql2Execute)
			throws ClassNotFoundException, SQLException {
		Connection conn = getConnection();
		conn.setAutoCommit(true);
	
		
		java.sql.Statement stmt = conn.createStatement();
		stmt.execute(sql2Execute);
		
		stmt.close();
		conn.close();
	}
	
	
	/**
	 * deletes all tables of the database
	 * @return true if delete was successfull
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public void flushDatabase() throws ClassNotFoundException, SQLException{
		Connection conn = getConnection();
		ResultSet res =  conn.getMetaData().getTables(null, null, null, new String[] {"TABLE"});
		List<String> tablesToDelete = new ArrayList<String>(); 
		while(res.next()){
			String tcat  = res.getString("TABLE_CAT"); 
	          String tschem =res.getString("TABLE_SCHEM");
	           String tname = res.getString("TABLE_NAME");
	           String ttype = res.getString("TABLE_TYPE");
	           String tremsarks = res.getString("REMARKS");
	           tablesToDelete.add(tname);
		}
		
		
		for (String tablename : tablesToDelete) {
			try {
				java.sql.Statement stmt = conn.createStatement();
				stmt.execute("DROP TABLE \"" + tablename +"\" CASCADE");
				stmt.close();
			} catch (Exception e) {
				//if an error occurs here, this should be ok 
			}
		}
	}
	
	/**
	 * compares the two files for equality
	 * @param outputLocation2
	 * @param referenceOutput2
	 * @return true if they are equal
	 * @throws FileNotFoundException 
	 */
	
	public boolean compare(String outputLocation, String referenceOutput) throws FileNotFoundException {
		
		Model m1 = ModelFactory.createDefaultModel();
		String fileSuffixout = outputLocation.substring(outputLocation.lastIndexOf(".")+1).toUpperCase();
		
		if(fileSuffixout.equals("NQ")){
			DatasetGraph dsgout = RiotLoader.load(outputLocation);
			DatasetGraph dsdref = RiotLoader.load(referenceOutput);
			
			if (dsgout.isEmpty() != dsdref .isEmpty()){
				  return false;
			}
			
			Iterator<Node> iout = dsgout.listGraphNodes();
			Iterator<Node> iref = dsdref.listGraphNodes();
		
			    while (iout.hasNext())
			    {
			      Node outNode = (Node)iout.next();
			      Graph outgraph =  dsgout.getGraph(outNode);
			      Graph refGRaf = dsdref.getGraph(outNode);
			      if (!outgraph.isIsomorphicWith(refGRaf))
			        return false;
			    }
			    return true;
			    
		}else {
		//if(fileSuffixout.equals("TTL")){
			m1.read(new FileInputStream(outputLocation),null,"TTL");
			Model m2 = ModelFactory.createDefaultModel();
			m2.read(new FileInputStream(referenceOutput),null,"TTL");
			
			if(m1.isIsomorphicWith(m2)){
				return true;
			}else{
				return false;
			}
		}
		
	
		

	}

}
