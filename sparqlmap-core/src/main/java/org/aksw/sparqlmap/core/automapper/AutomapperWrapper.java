package org.aksw.sparqlmap.core.automapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.SQLException;

import javax.annotation.PostConstruct;

import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.db.DBAccessConfigurator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.hp.hpl.jena.rdf.model.Model;

/**
 * the automapper was created in a separate project. This wrapper is there for historical reasons.
 * 
 * @author joerg
 * 
 */

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
  private void init() {
    baseUri = env.getProperty("sm.baseuri");
    dmR2rmlDump = env.getProperty("sm.dmr2rmldump");
  }
  
  /**
   * Based on the configuration given, a direct mapping R2RML document is created.
   * @return the model with the R2RML mapping
   * @throws SQLException thrown, if the db exploration fails
   * @throws FileNotFoundException thrown, if the file cold not be written.
   */
  public Model automap() throws SQLException, FileNotFoundException {
    Connection conn = this.dbaccess.getConnection();

    Automapper automapper = new Automapper(conn, baseUri, baseUri, baseUri, ";");
    Model dmR2rml = automapper.getMydbData();
    if (dmR2rmlDump != null && !dmR2rml.isEmpty()) {
      dmR2rml.write(new FileOutputStream(new File(dbconf.getDBName() + "-dm.ttl")), "TTL");
    }
    conn.close();
    return dmR2rml;
  }

}
