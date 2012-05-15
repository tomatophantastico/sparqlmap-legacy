package org.aksw.sparqlmap.config.syntax.r2rml;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.schema.Table;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

public class PG22R2RML {
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(PG22R2RML.class);
	
	private String baseInstanceUri;
	private String baseVocabUri;
	private String schemaName;
	private BoneCP connectionPool;
	
	private R2RMLModel model;
	

	
	
	public PG22R2RML(String connectionString, String user, String password, String schemaName, String baseInstanceUri,String baseVocabUri) {
		
		
		try {
			Class.forName("org.postgresql.Driver");

			// setup the connection pool
			BoneCPConfig config = new BoneCPConfig();
			config.setJdbcUrl(connectionString); // jdbc url specific to your database, eg jdbc:mysql://127.0.0.1/yourdb
			config.setUsername(user); 
			config.setPassword(password);
			config.setMinConnectionsPerPartition(1);
			config.setMaxConnectionsPerPartition(5);
			config.setPartitionCount(1);
			connectionPool = new BoneCP(config); // setup the connection pool

			//setupDriver(dbconf.getDbConnString(),dbconf.getUsername(),dbconf.getPassword());
		} catch (Exception e) {
			log.error("Error setting up the db pool",e);
		}
		
		model = new R2RMLModel(baseVocabUri,baseInstanceUri);
		
		
	}
	
	
	public String toR2RML() throws SQLException{
		Set<TripleMap> tripleMaps =  	createTripleMaps();
		addFKRelations();
		
		
		StringBuffer out = new StringBuffer();
		for (TripleMap tripleMap : tripleMaps) {
			tripleMap.toTtl(out);
		}
		
		return out.toString();
	}
	
	
	private Set<TripleMap> createTripleMaps() throws SQLException {
		List<String> tables = getTableNames();
		Set<TripleMap> tripleMaps = new HashSet<TripleMap>();
		for (String table : tables) {
			tripleMaps.add(createTermMaps(table, schemaName));
		}
		
		return tripleMaps;
		
	}


	private void addFKRelations() {
		// TODO Auto-generated method stub
		
	}


	private List<String> getTableNames() throws SQLException{
		List<String> names = new ArrayList<String>();
		
		String q = "SELECT table_name FROM information_schema.tables WHERE table_schema = '" +schemaName+"'";
		
		Connection conn = connectionPool.getConnection();
		Statement stmt = conn.createStatement();
		ResultSet rs  = stmt.executeQuery(q);
		while(rs.next()){
			names.add(rs.getString("table_name"));
		}
		rs.close();
		stmt.close();
		conn.close();
		
		
		return names;
	}
	
	private TripleMap createTermMaps(String table, String schemaName) throws SQLException{
		LinkedHashSet<Col> cols = new LinkedHashSet<Col>();
		
		
		String q = "select column_name, ordinal_position, data_type, is_identity from information_schema.columns  where table_schema = '"+schemaName+" AND table_name = '"+table+"' ORDER BY ordinal_position ASC";
		Connection conn = connectionPool.getConnection();
		Statement stmt = conn.createStatement();
		ResultSet rs  = stmt.executeQuery(q);
		while(rs.next()){
			
			Col col = new Col();
			
			col.name = rs.getString("column_name");
			col.isId = rs.getBoolean("is_identity");
			
			cols.add(col);	
		}
		rs.close();
		stmt.close();
		conn.close();
		
		LinkedHashSet<Col> ids = new LinkedHashSet<PG22R2RML.Col>();
		for(Col col: cols){
			if(col.isId == true){
				ids.add(col);
			}
		}
		if(ids.isEmpty()){
			ids.add( cols.iterator().next());
			
		}
		cols.removeAll(ids);
		
		Table fromItem = new Table(null, table);
		
		TripleMap triMap = new TripleMap(fromItem);
		
				
		
		//create now the termMap for subject out of all subject ids
		Iterator<Col> icol = ids.iterator();
		
		while(icol.hasNext()){
			
		}
		
		
		
		
		
		
		
		
		return triMap;
		
		
		

	}
	
	
	private class Col{
		private String name;
		private Boolean isId;
	}
	
	
	
	
	
	
	
	
	
	
	public static void main(String[] args) throws SQLException {
		PG22R2RML p2r = new PG22R2RML(args[0], args[1], args[2], args[3], args[4],args[5]);
		
		System.out.print(p2r.toR2RML());
		
	}
	

	

}
