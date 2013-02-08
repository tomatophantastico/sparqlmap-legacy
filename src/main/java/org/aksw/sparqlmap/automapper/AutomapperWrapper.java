package org.aksw.sparqlmap.automapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.SQLException;

import javax.annotation.PostConstruct;

import org.aksw.sparqlmap.db.DBAccess;
import org.aksw.sparqlmap.db.DBAccessConfigurator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.hp.hpl.jena.rdf.model.Model;



@Component
public class AutomapperWrapper {
	
	@Autowired
	private DBAccess dbaccess;
	
	@Autowired
	private DBAccessConfigurator dbconf;
	
	@Autowired
	private Environment env;

	private String baseUri;

	private String dmR2rmlDump;
	
	@PostConstruct
	public void loadBaseUri() throws SQLException{
		baseUri = env.getProperty("sm.baseuri");
		dmR2rmlDump = env.getProperty("sm.dmr2rmldump");
	}

	
	
	public Model automap() throws SQLException, FileNotFoundException{
		Connection conn = this.dbaccess.getConnection();
		
		DB2R2RML automapper = new DB2R2RML(conn,baseUri,baseUri,baseUri,";");
		Model dmR2rml = automapper.getMydbData();
		if(dmR2rmlDump!=null&&!dmR2rml.isEmpty()){
			dmR2rml.write(new FileOutputStream(new File(dbconf.getJdbcDBName()+"-dm.ttl")), "TTL");
		}
		conn.close();
		return dmR2rml;
	}
	

}
