package org.aksw.sparqlmap.core.automapper;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import javax.print.URIException;

import org.aksw.commons.util.jdbc.ForeignKey;
import org.aksw.commons.util.jdbc.Schema;
import org.aksw.sparqlmap.core.config.syntax.r2rml.R2RML;
import org.springframework.web.util.UriComponentsBuilder;

import com.google.common.collect.Multimap;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

/**
 * This class generates an R2RML mapping based on the direct mapping specification out of a database.
 * 
 * @author joerg
 * @author sherif
 *
 */

public class Automapper {

  static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Automapper.class);

  
  private Model r2rmlGraph = ModelFactory.createDefaultModel();

  private Resource tripleMap;

  private String dbUri, dataUri, vocUri, compPkSep;

  // private Properties dbProps;
  private Connection dbConnction;

  private DatabaseMetaData md;

  private Schema dbSchema;


  public Automapper(Connection conn, String mappedDbUri, String userDataUri, String userVocUri, String userCompPkSep)
    throws SQLException {
    this.dbConnction = conn;
    this.md = dbConnction.getMetaData();
    this.dbUri = mappedDbUri != null ? mappedDbUri : "http://example.com/mapping/";
    this.dataUri = userDataUri != null ? userDataUri : "http://example.com/data/";
    this.vocUri = userVocUri != null ? userVocUri : "http://example.com/vocabulary/";
    this.compPkSep = userCompPkSep != null ? userCompPkSep : ";";
    this.dbSchema = Schema.create(dbConnction);

  }

  public Model getMydbData() throws SQLException {

    try {

      java.sql.ResultSet catalogs = null;
      catalogs = md.getCatalogs();
      java.sql.ResultSet primaryKeyRecordSet = null;
      String[] TABLE_TYPES = { "TABLE" };
      catalogs = md.getTables(null, null, "%", TABLE_TYPES);

      catalogs.beforeFirst();
      while (catalogs.next()) {
        String tableName = catalogs.getString(3); // "TABLE_CATALOG"
        mapTable(tableName);
        // PK fields
        primaryKeyRecordSet = md.getPrimaryKeys(null, null, tableName);

        // Map PKs
        mapPrimaryKey(primaryKeyRecordSet, tableName);

        // FK Fields
        mapForeignKeys(tableName);

        // Normal fields
        mapAllKeys(tableName);

      }

      r2rmlGraph.setNsPrefix("rr", R2RML.R2RML_STRING);
      r2rmlGraph.setNsPrefix("vocab", vocUri);
      r2rmlGraph.setNsPrefix("mapping", dbUri);
      r2rmlGraph.setNsPrefix("data", dataUri);
      // r2rmlGraph.write(System.out,"N-TRIPLE");
      // return r2rmlGraph;
    } catch (UnsupportedEncodingException e) {
      log.error("Error:", e);
    }
    return r2rmlGraph;
  }

  private String urlEncode(String tableName) throws UnsupportedEncodingException {

    return UriComponentsBuilder.newInstance().path(tableName).build().encode().toString();
    // URLEncoder.encode(tableName, "UTF-8").replaceAll("\\+", "%20");
  }

  String getParentTable(ArrayList<String> tableNamesList, String fkName) {
    String tableName = null;
    for (int i = 0; i < tableNamesList.size(); i++) {
      tableName = tableNamesList.get(i);
      if (fkName.contains(tableName)) {
        return tableName;
      }
    }
    return fkName;
  }

  /**
   * mapTable maps the rr:logicalTabel property and table name.
   * 
   * @param tableName
   * @param tableNameEncoded
   * @author Sherif
   * @throws UnsupportedEncodingException
   */
  void mapTable(String tableName) throws UnsupportedEncodingException {
    String tableNameEncoded = "";
    tableNameEncoded = urlEncode(tableName);
    tripleMap = r2rmlGraph.createResource(dbUri + tableNameEncoded);
    Resource tabelNameResource = r2rmlGraph.createResource().addProperty(R2RML.tableName, '\"' + tableName + '\"');
    tripleMap.addProperty(R2RML.logicalTable, tabelNameResource);
  }

  /**
   * getPrimaryKeyCount tack DatabaseMetaData and a pacific table name as parameters and returns the number of primary
   * keys in this table
   * 
   * @param DatabaseMetaData
   * @param tableName
   * @return Primary Keys Count
   * @author Sherif
   * @throws SQLException
   */
  int getPrimaryKeyCount(String tableName) throws SQLException {
    java.sql.ResultSet primaryKeyRecordSet = null;
    int rowcount = 0;
    primaryKeyRecordSet = md.getPrimaryKeys(null, null, tableName);
    if (primaryKeyRecordSet.last()) {
      rowcount = primaryKeyRecordSet.getRow();
      primaryKeyRecordSet.beforeFirst(); // not primaryKeyRecordSet.first() because the primaryKeyRecordSet.next() below
                                         // will move on, missing the first element
    }
    return rowcount;
  }

  /**
   * mapPrimaryKey maps rr:subjectMap, rr:class and rr:template properties for Primary Key Table
   * 
   * @param primaryKeyRecordSet
   * @param pKcolumnName
   * @param tableNameEncoded
   * @author Sherif
   * @throws SQLException
   * @throws UnsupportedEncodingException
   */
  void mapPrimaryKey(java.sql.ResultSet primaryKeyRecordSet, String tableName) throws UnsupportedEncodingException,
    SQLException {
    // Count PKs
    int primaryKeysCount = getPrimaryKeyCount(tableName);
    String tableNameEncoded = urlEncode(tableName);

    if (primaryKeysCount > 1) { // case composite PKs
      mapCompositePrimaryKey(primaryKeyRecordSet, tableNameEncoded);
    } else if (primaryKeysCount == 1) { // case 1 PK
      mapSinglePrimaryKey(primaryKeyRecordSet, tableNameEncoded);
    } else { // case No PK
      mapNoPrimaryKey(tableName);
    }
  }

  /**
   * mapCompositePrimaryKey maps rr:subjectMap, rr:class and rr:template properties for Composite Primary Key Table
   * 
   * @param primaryKeyRecordSet
   * @param pKcolumnName
   * @param tableNameEncoded
   * @author Sherif
   * @throws SQLException
   * @throws UnsupportedEncodingException
   */
  void mapCompositePrimaryKey(java.sql.ResultSet primaryKeyRecordSet, String tableNameEncoded)
    throws UnsupportedEncodingException, SQLException {
    String compositePrimaryKeyString = "";

    while (primaryKeyRecordSet.next()) {
      String pKcolumnName = primaryKeyRecordSet.getString("COLUMN_NAME");
      String pKcolumnNameEncoded = urlEncode(pKcolumnName);
      compositePrimaryKeyString += pKcolumnNameEncoded + "={\"" + pKcolumnName + "\"}";
      compositePrimaryKeyString += primaryKeyRecordSet.isLast() ? "" : compPkSep;
    }

    Property VocUriProperty = ResourceFactory.createProperty(vocUri + tableNameEncoded);

    Resource dataVocUriResource =
      r2rmlGraph.createResource()
        .addProperty(R2RML.template, dataUri + tableNameEncoded + "/" + compositePrimaryKeyString)
        .addProperty(R2RML.hasClass, VocUriProperty);

    tripleMap.addProperty(R2RML.subjectMap, dataVocUriResource);
  }

  /**
   * mapSinglePrimaryKey maps rr:subjectMap, rr:class and rr:template properties for Single PrimaryKey Table
   * 
   * @param primaryKeyRecordSet
   * @param pKcolumnName
   * @param tableNameEncoded
   * @author Sherif
   * @throws SQLException
   * @throws UnsupportedEncodingException
   */
  void mapSinglePrimaryKey(java.sql.ResultSet primaryKeyRecordSet, String tableNameEncoded) throws SQLException,
    UnsupportedEncodingException {
    String pKcolumnName = "";
    String pKcolumnNameEncoded = "";

    primaryKeyRecordSet.first();
    pKcolumnName = primaryKeyRecordSet.getString("COLUMN_NAME");
    pKcolumnNameEncoded = urlEncode(pKcolumnName);
    Property VocUriProperty = ResourceFactory.createProperty(vocUri + tableNameEncoded);

    Resource dataVocUriResource =
      r2rmlGraph
        .createResource()
        .addProperty(R2RML.template,
          dataUri + tableNameEncoded + '/' + pKcolumnNameEncoded + "={\"" + pKcolumnName + "\"}")
        .addProperty(R2RML.hasClass, VocUriProperty);

    tripleMap.addProperty(R2RML.subjectMap, dataVocUriResource);
  }

  /**
   * mapNoPrimaryKey maps rr:subjectMap, rr:class rr:BlankNode and rr:template properties for no PrimaryKey Table
   * 
   * @param primaryKeyRecordSet
   * @param pKcolumnName
   * @param tableNameEncoded
   * @author Sherif
   * @throws SQLException
   * @throws UnsupportedEncodingException
   */
  void mapNoPrimaryKey(String tableName) throws SQLException, UnsupportedEncodingException {
    /**
     * If no PK then make a PK as a composition of all keys
     */
    String compositePrimaryKeyString = "";
    java.sql.ResultSet r;
    String tableNameEncoded = "";
    r = md.getColumns(null, null, tableName, "%");
    tableNameEncoded = urlEncode(tableName);

    while (r.next()) {
      String columnName = r.getString(4);
      compositePrimaryKeyString += tableNameEncoded + "={\"" + columnName + "\"}";
      compositePrimaryKeyString += r.isLast() ? "" : compPkSep;
    }

    
    Property VocUriProperty = ResourceFactory.createProperty(vocUri + tableNameEncoded);

    Resource dataVocUriBnodeResource = r2rmlGraph.createResource();
    dataVocUriBnodeResource.addProperty(R2RML.template, dataUri + compositePrimaryKeyString)
      .addProperty(R2RML.hasClass, VocUriProperty).addProperty(R2RML.termType, R2RML.BlankNode);

    tripleMap.addProperty(R2RML.subjectMap, dataVocUriBnodeResource);
  }

  /**
   * mapAllKeys maps rr:predicateObjectMap, rr:predicate rr:objectMap and rr:column properties for all keys of each
   * Table
   * 
   * @param primaryKeyRecordSet
   * @param pKcolumnName
   * @param tableNameEncoded
   * @author Sherif
   * @throws SQLException
   * @throws UnsupportedEncodingException
   */
  void mapAllKeys(String tableName) throws SQLException, UnsupportedEncodingException {

    java.sql.ResultSet allKeysRecordSet = md.getColumns(null, null, tableName, "%");

    while (allKeysRecordSet.next()) {
      String colName = allKeysRecordSet.getString(4);
      String colNameEncoded = urlEncode(colName);
      String tableNameEncoded = urlEncode(tableName);

      //Property predicateObjectMapProperty = ResourceFactory.createProperty(RR + "predicateObjectMap");
      //Property predicateMapProperty = ResourceFactory.createProperty(RR + "predicate");
      Property vocUriProperty = ResourceFactory.createProperty(vocUri + tableNameEncoded + '#' + colNameEncoded);
      //Property objectMapProperty = ResourceFactory.createProperty(RR + "objectMap");
      //Property columnProperty = ResourceFactory.createProperty(RR + "column");

      Resource columnNameResource = r2rmlGraph.createResource();
      columnNameResource.addProperty(R2RML.column, '\"' + colName + '\"');

      Resource VocUriColResource = r2rmlGraph.createResource();
      VocUriColResource.addProperty(R2RML.predicate, vocUriProperty).addProperty(R2RML.objectMap,
        columnNameResource);

      tripleMap.addProperty(R2RML.predicateObjectMap, VocUriColResource);
    }
  }

  /**
   * mapForeignKeys maps rr:predicateObjectMap, rr:predicate rr:objectMap, rr:parentTriplesMap, rr:joinCondition,
   * rr:child and rr:parent properties foreign keys of each Table
   * 
   * @param tableName
   * @author sherif
   * @throws UnsupportedEncodingException
   * @throws SQLException
   */
  void mapForeignKeys(String tableName) throws UnsupportedEncodingException, SQLException {
    String tableNameEncoded = urlEncode(tableName);

    Multimap<String, ForeignKey> foreinKeysMultimap = dbSchema.getForeignKeys();
    Collection<ForeignKey> foreinKeysCollection = foreinKeysMultimap.get(tableName);


    for (ForeignKey fk : foreinKeysCollection) {// if the mapping related to the current table do

     
      Property dbUriProperty = ResourceFactory.createProperty(dbUri + urlEncode(fk.getTarget().getTableName()));

      // get the composite Keys String
      String compositFkString = "";
      for (int compFkIndex = 0; compFkIndex < fk.getSource().getColumnNames().size(); compFkIndex++) {
        String fkSubStringEncoded = urlEncode(fk.getSource().getColumnNames().get(compFkIndex));
        compositFkString += fkSubStringEncoded + ";";
      }
      compositFkString = compositFkString.substring(0, compositFkString.length() - 1); // remove the last ";"
      Property vocUriProperty = ResourceFactory.createProperty(vocUri + tableNameEncoded + "#ref-" + compositFkString);

      Resource[] childParentResource = new Resource[fk.getSource().getColumnNames().size()];
      Resource parentTriplesMapjoinConditionResource = null;
      Resource predicateobjectMapResource = null;

      for (int childParentResourceIndex = 0; childParentResourceIndex < fk.getSource().getColumnNames().size(); childParentResourceIndex++) {
        childParentResource[childParentResourceIndex] =
          r2rmlGraph.createResource()
            .addProperty(R2RML.child, '\"' + fk.getSource().getColumnNames().get(childParentResourceIndex) + '\"')
            .addProperty(R2RML.parent, '\"' + fk.getTarget().getColumnNames().get(childParentResourceIndex) + '\"');
      }

      parentTriplesMapjoinConditionResource = r2rmlGraph.createResource();
      parentTriplesMapjoinConditionResource.addProperty(R2RML.parentTriplesMap, dbUriProperty);
      for (int childParentResourceIndex = 0; childParentResourceIndex < fk.getSource().getColumnNames().size(); childParentResourceIndex++) {
        parentTriplesMapjoinConditionResource.addProperty(R2RML.joinCondition,
          childParentResource[childParentResourceIndex]);
      }

      predicateobjectMapResource =
        r2rmlGraph.createResource().addProperty(R2RML.predicate, vocUriProperty)
          .addProperty(R2RML.objectMap, parentTriplesMapjoinConditionResource);

      tripleMap.addProperty(R2RML.predicateObjectMap, predicateobjectMapResource);

    }
  }

}
