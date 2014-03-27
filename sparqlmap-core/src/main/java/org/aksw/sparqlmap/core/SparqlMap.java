package org.aksw.sparqlmap.core;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.aksw.sparqlmap.core.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.mapper.Mapper;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.WebContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.sparql.resultset.ResultsFormat;
import com.hp.hpl.jena.sparql.syntax.Template;

/**
 *  The main class of Sparqlmap.
 *  Provides methods for executing SPARQL-queries over mapped databases. 
 * @author joerg
 *
 */
@Component
public class SparqlMap {
  
  private static Logger log = LoggerFactory.getLogger(SparqlMap.class);

  /** total queries translated by this instance.*/
  private Integer querycount = 0;

  private String baseUri;

  private boolean continueWithInvalidUris = true;

  @Autowired
  private Environment env;

  @Autowired
  private Mapper mapper;

  @Autowired
  private R2RMLModel mapping;

  @Autowired
  private DBAccess dbConf;

  
  @PostConstruct
  public void init() {
    baseUri = env.getProperty("sm.baseuri");
    continueWithInvalidUris = new Boolean(env.getProperty("sm.continuewithinvaliduris", "true"));
  }

  /**
   * Returns the result of a SPARQL query as a String.
   * 
   * @param qstring
   * @param rt
   * @return
   * @throws SQLException
   */
  public String executeSparql(String qstring, Object rt) throws SQLException {
    ByteArrayOutputStream resBos = new ByteArrayOutputStream();
    executeSparql(qstring, rt, resBos);
    return resBos.toString();
  }

  /**
   * Executes a SPARQL query and writes its result into the Outputstream.
   * 
   * @param qstring the query
   * @param rt the Return type, either as Lang or as ResultsFormat.
   * @param out the stream into which the result gets written into.
   * @throws SQLException thrown if an db error occurs.
   */
  public void executeSparql(String qstring, Object rt, OutputStream out) throws SQLException {
    executeSparql(qstring, rt, out, "Unnamed query " + this.querycount++);
  }

  /**
   * Takes some of the functionality of QueryExecutionbase.
   * 
   * @param queryname
   *          an query name, just for the log.
   * @param qstring
   *          the SPARQL query that should be executed
   * @param out
   *          the result gets printed into this stream.
   * @throws SQLException thrown if an db error occurs.
   */
  protected void executeSparql(String qstring, Object rf, OutputStream out, String queryname) throws SQLException {

    TranslationContext context = new TranslationContext();
    context.setQueryString(qstring);
    context.setQueryName(queryname);
    context.profileStartPhase("Query Compile");
    context.setQuery(QueryFactory.create(qstring));
    context.setTargetContentType(rf);

    try {
      if (context.getQuery().isAskType()) {

        context.getQuery().setLimit(1);
        ResultSet rs = executeSelect(context);
        if (rs.hasNext()) {
          ResultSetFormatter.out(out, true);
        } else {
          ResultSetFormatter.out(out, false);
        }

      }
      if (context.getQuery().isConstructType()) {

        if (context.getTargetContentType() != null && context.getTargetContentType().equals(Lang.NTRIPLES)) {
          executeConstruct(context, out);
        } else {
          Model model = executeConstruct(context);
          RDFDataMgr.write(out, model, (Lang) context.getTargetContentType());

        }

      }
      if (context.getQuery().isSelectType()) {
        ResultSet rs = executeSelect(context);

        if (context.getTargetContentType() == null) {
          context.setTargetContentType(ResultsFormat.FMT_RDF_XML);
        }

        ResultSetFormatter.output(out, rs, (ResultsFormat) context.getTargetContentType());

      }
      if (context.getQuery().isDescribeType()) {

        if (context.getTargetContentType() == null) {
          context.setTargetContentType(Lang.TURTLE);
        }

        Model model = ModelFactory.createDefaultModel();
        List<Node> iris = context.getQuery().getResultURIs();
        if ((iris == null || iris.isEmpty())) {
          Var var = context.getQuery().getProjectVars().get(0);

          // hacky, hacky, hacky

          ResultSet rs = executeSelect(context);
          while (rs.hasNext()) {
            iris.add(rs.next().get(var.getName()).asNode());
          }

        }

        for (Node node : iris) {
          String con1 =
            "CONSTRUCT {?s_sm ?p_sm <" + node.getURI() + "> } WHERE { ?s_sm ?p_sm <" + node.getURI() + "> }";
          TranslationContext subCon1 = new TranslationContext();
          subCon1.setTargetContentType(context.getTargetContentType());
          subCon1.setQueryString(con1);
          subCon1.setQueryName("construct incoming query");
          subCon1.setQuery(QueryFactory.create(con1));

          model.add(executeConstruct(subCon1));
          String con2 = "CONSTRUCT { <" + node.getURI() + "> ?p_sm ?o_sm} WHERE { <" + node.getURI() + "> ?p_sm ?o_sm}";
          TranslationContext subCon2 = new TranslationContext();
          subCon2.setTargetContentType(context.getTargetContentType());
          subCon2.setQueryString(con2);
          subCon2.setQuery(QueryFactory.create(con2));
          subCon2.setQueryName("construct outgoing query");

          model.add(executeConstruct(subCon2));

        }

        RDFDataMgr.write(out, model, (Lang) context.getTargetContentType());

      }

    } catch (Throwable e) {
      log.error("An error occured while translating\n\n " + context.toString(), e);
      throw e;
    }

  }

  public Model executeConstruct(String query) throws SQLException {
    TranslationContext context = new TranslationContext();
    context.setQueryString(query);
    try {

      context.setQuery(QueryFactory.create(query));
      return executeConstruct(context);
    } catch (Exception e) {
      context.setProblem(e);
      log.error(context.toString());
      throw e;
    }

  }

  public Model executeConstruct(TranslationContext context) throws SQLException {
    Model model = ModelFactory.createDefaultModel();
    Template template = context.getQuery().getConstructTemplate();
    context.getQuery().setQueryResultStar(true);
    // execute it

    ResultSet rs = executeSelect(context);

    while (rs.hasNext()) {
      Set<Triple> generatedTriples = new HashSet<Triple>();
      Map<Node, Node> bNodeMap = new HashMap<Node, Node>();
      Binding binding = rs.nextBinding();
      template.subst(generatedTriples, bNodeMap, binding);

      for (Triple generatedTriple : generatedTriples) {
        if (generatedTriple.isConcrete()) {
          model.getGraph().add(generatedTriple);
        } else {
          log.warn("Unconcrete triple created by template, skipping: " + generatedTriple.toString());
        }

      }

    }

    return model;

  }

  /**
   * performing an construct like this does build up an in-memory representation of the data and streams it right to the
   * client.
   * 
   * Use only with ntriples.
   * 
   * @param context
   * @param out
   * @throws SQLException
   */
  public void executeConstruct(TranslationContext context, OutputStream out) throws SQLException {
    // take the graph pattern and convert it into a select query.
    Template template = context.getQuery().getConstructTemplate();
    context.getQuery().setQueryResultStar(true);
    // execute it
    ResultSet rs = executeSelect(context);

    // bind it
    int i = 0;
    Graph graph = GraphFactory.createDefaultGraph();
    while (rs.hasNext()) {
      Set<Triple> set = new HashSet<Triple>();
      Map<Node, Node> bNodeMap = new HashMap<Node, Node>();
      Binding binding = rs.nextBinding();
      template.subst(set, bNodeMap, binding);

      for (Triple t : set) {
        graph.add(t);

      }

      if (++i % 1000 != 0) {
        RDFDataMgr.write(out, graph, WebContent.contentTypeToLang(context.getTargetContentType().toString()));
      }
    }
    RDFDataMgr.write(out, graph, WebContent.contentTypeToLang(context.getTargetContentType().toString()));

  }

  /**
   * dumps into the whole config into the writer.
   * 
   * @param writer
   * @throws SQLException
   */

  public void dump(OutputStream out, String format) throws SQLException {
    PrintStream writer = new PrintStream(out);

    List<TranslationContext> contexts = mapper.dump();
    for (TranslationContext context : contexts) {

      log.info("SQL: " + context.getSqlQuery());
      com.hp.hpl.jena.query.ResultSet rs = dbConf.executeSQL(context, baseUri);
      DatasetGraph graph = DatasetGraphFactory.createMem();
      boolean usesGraph = false;
      int i = 0;
      while (rs.hasNext()) {
        Binding bind = rs.nextBinding();
        if (bind.get(Var.alloc("g")) != null) {

          usesGraph = true;
        }
        try {
          Quad toadd =
            new Quad(bind.get(Var.alloc("g")), bind.get(Var.alloc("s")), bind.get(Var.alloc("p")), bind.get(Var
              .alloc("o")));
          graph.add(toadd);
        } catch (Exception e) {

          log.error("Error:", e);
          if (!continueWithInvalidUris) {
            throw new RuntimeException(e);
          }
        }
        if (++i % 1000 == 0) {
          if (usesGraph) {
            RDFDataMgr.write(out, graph, RDFFormat.NQUADS);
            graph.deleteAny(null, null, null, null);
          } else {
            RDFDataMgr.write(out, graph.getDefaultGraph(), Lang.NTRIPLES);
            graph.deleteAny(null, null, null, null);

          }
        }
      }
      if (usesGraph) {
        RDFDataMgr.write(out, graph, RDFLanguages.nameToLang(format));
      } else {
        RDFDataMgr.write(out, graph.getDefaultGraph(), RDFLanguages.nameToLang(format));
      }

      writer.flush();
    }
  }

  /**
   * dumps into the whole config into the writer.
   * 
   * @param writer
   * @throws SQLException
   */

  public DatasetGraph dump() throws SQLException {

    DatasetGraph dataset = DatasetGraphFactory.createMem();

    List<TranslationContext> contexts = mapper.dump();
    for (TranslationContext context : contexts) {
      log.info("SQL: " + context.getSqlQuery());
      com.hp.hpl.jena.query.ResultSet rs = dbConf.executeSQL(context, baseUri);
      while (rs.hasNext()) {
        Binding bind = rs.nextBinding();
        try {
          Quad toadd =
            new Quad(bind.get(Var.alloc("g")), bind.get(Var.alloc("s")), bind.get(Var.alloc("p")), bind.get(Var
              .alloc("o")));
          dataset.add(toadd);
        } catch (Exception e) {

          log.error("Error:", e);
          if (!continueWithInvalidUris) {
            throw new RuntimeException(e);
          }
        }
      }
    }
    return dataset;
  }

  public ResultSet executeSelect(String query) throws SQLException {

    TranslationContext context = new TranslationContext();
    context.setQueryString(query);
    context.setQuery(QueryFactory.create(query));

    return executeSelect(context);

  }

  public ResultSet executeSelect(TranslationContext context) throws SQLException {

    try {
      context.profileStartPhase("Rewriting");

      mapper.rewrite(context);

      LoggerFactory.getLogger("sqllog").info("SQL " + context.getQueryName() + " " + context.getSqlQuery());

      ResultSet rs = dbConf.executeSQL(context, baseUri);

      return rs;

    } catch (Throwable e) {
      context.setProblem(e);

      log.error(context.toString());

      throw e;
    }

  }

  protected Mapper getMapper() {
    return mapper;
  }
}
