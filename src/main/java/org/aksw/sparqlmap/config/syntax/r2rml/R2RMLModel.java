package org.aksw.sparqlmap.config.syntax.r2rml;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SelectBodyString;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.aksw.sparqlmap.config.syntax.r2rml.TripleMap.PO;
import org.aksw.sparqlmap.db.IDBAccess;
import org.aksw.sparqlmap.mapper.compatibility.CompatibilityChecker;
import org.aksw.sparqlmap.mapper.compatibility.SimpleCompatibilityChecker;
import org.aksw.sparqlmap.mapper.compatibility.columnanalyze.CompatibilityCheckerFactory;
import org.aksw.sparqlmap.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.mapper.translate.ImplementationException;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.update.GraphStoreFactory;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.vocabulary.RDF;

public class R2RMLModel {

	public R2RMLModel(ColumnHelper columnhelper, IDBAccess dbconf,
			DataTypeHelper dth, Model mapping, Model r2rmlSchema) {
		super();
		this.columnhelper = columnhelper;
		this.dbconf = dbconf;
		this.dth = dth;
		this.mapping = mapping;
		this.r2rmlSchema = r2rmlSchema;
	}

	private ColumnHelper columnhelper;
	private IDBAccess dbconf;
	private DataTypeHelper dth;
	Model mapping = null;
	Model r2rmlSchema = null;
	
	private boolean validate = true;
	private boolean validateDeep = true;

	Map<String, Map<String, String>> col2castTo = new HashMap<String, Map<String, String>>();

	static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(R2RMLModel.class);

	Model reasoningModel = null;
	Map<String, TripleMap> tripleMaps = null;

	@PostConstruct
	public void setup() throws R2RMLValidationException, JSQLParserException,
			SQLException {
		reasoningModel = ModelFactory.createRDFSModel(r2rmlSchema, mapping);
		resolveRRClassStatements();
		resolveR2RMLShortcuts();
		validate();
		
		loadTripleMaps();
		loadParentTripleStatements();

		loadCompatibilityChecker();
		validatepost();
	}

	private void loadParentTripleStatements() {

		String ptquery = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> \n"
				+ "SELECT DISTINCT * \n" + "{\n"
				+ "?tmuri rr:predicateObjectMap ?refPredObjMap. \n"
				+ " ?refPredObjMap rr:objectMap ?refObjectMap.\n"
				+ " ?refPredObjMap rr:predicateMap ?pm.\n"
				+ getTermMapQuery("p") + "?refObjectMap a rr:RefObjectMap.\n"
				+ "?refObjectMap rr:parentTriplesMap ?parentTmUri. \n" + "\n"
				+ "}";

		ResultSet tmrs = QueryExecutionFactory.create(
				QueryFactory.create(ptquery), reasoningModel).execSelect();


		while (tmrs.hasNext()) {

			QuerySolution sol = tmrs.next();

			String tmUri = sol.get("tmuri").asResource().getURI();
			TripleMap tm = this.tripleMaps.get(tmUri);
			String parentTripleMapUri = sol.get("parentTmUri").asResource()
					.getURI();
			TripleMap parentTm = this.tripleMaps.get(parentTripleMapUri);

			TermMap newTermMap = parentTm.getSubject().clone("");

			log.info("Triple Map " + tmUri + " has a parent Map:"
					+ parentTripleMapUri);

			// get join conditions

			String joinConditionQuery = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> \n"
					+ "SELECT * \n"
					+ "{\n"
					+ "<"
					+ tmUri
					+ "> rr:predicateObjectMap ?refPredObjMap. \n"
					+ " ?refPredObjMap rr:objectMap ?refObjectMap.\n"
					+ "?refObjectMap a rr:RefObjectMap.\n"
					+ "?refObjMap rr:parentTriplesMap <"
					+ parentTripleMapUri
					+ ">. \n"
					+ "?refObjMap rr:joinCondition ?jc. ?jc rr:child ?jcchild. ?jc rr:parent ?jcparent"
					+ "\n" + "}";

			ResultSet jcrs = QueryExecutionFactory.create(
					QueryFactory.create(joinConditionQuery), reasoningModel)
					.execSelect();

			while (jcrs.hasNext()) {

				QuerySolution jcsol = jcrs.next();

				String parentjcColName = jcsol.get("jcparent").asLiteral()
						.toString();

				// validate it
				this.dbconf.getDataType(parentTm.from,
						getRealColumnName(parentjcColName, parentTm.from));
				Table tab = new Table(null, parentTm.from.getAlias());
				tab.setAlias(parentTm.from.getAlias());
				Column leftCol = new Column(tab, parentjcColName);

				String childjcColName = jcsol.get("jcchild").asLiteral()
						.toString();
				Table table = new Table(null, tm.from.getAlias());
				table.setAlias(tm.from.getAlias());
				Column rightCol = new Column(table, childjcColName);
				EqualsTo eq = new EqualsTo();
				eq.setLeftExpression(dth.cast(leftCol, dth.getStringCastType()));
				eq.setRightExpression(dth.cast(rightCol,
						dth.getStringCastType()));
				newTermMap.getFromJoins().add(eq);
				newTermMap.addFromItem(tm.from);

				log.info("And joins on parent: " + parentTm.from.toString()
						+ "." + parentjcColName + " and " + tm.from.toString()
						+ "." + childjcColName);
			}

			TermMap ptm = null;

			TermMapQueryResult ptmrs = new TermMapQueryResult(sol, "p", tm.from);

			// some general validation
			if (ptmrs.termType != null
					&& !ptmrs.termType.getURI().equals(R2RML.IRI)) {
				throw new R2RMLValidationException(
						"Only use iris in predicate position");
			}

			ptm = createTermMap(tm.from, ResourceFactory.createResource(tmUri),
					tm, null, ptmrs, ColumnHelper.COL_VAL_TYPE_RESOURCE);

			tm.addPO(ptm, newTermMap);

		}

	}

	private void resolveRRClassStatements() {
		String query = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> "
				+ "INSERT { ?tm rr:predicateObjectMap  _:newpo. "
				+ "_:newpo rr:predicate <" + RDF.type.getURI() + ">."
				+ "_:newpo rr:object ?class } " + "WHERE {?tm a rr:TriplesMap."
				+ "?tm  rr:subjectMap ?sm." + "?sm rr:class ?class }";
		UpdateExecutionFactory.create(UpdateFactory.create(query),
				GraphStoreFactory.create(reasoningModel)).execute();
	}

	private void loadCompatibilityChecker() throws SQLException {
//		CompatibilityCheckerFactory ccfac = new CompatibilityCheckerFactory(
//				reasoningModel, dbconf);

		for (TripleMap tripleMap : tripleMaps.values()) {
			CompatibilityChecker ccs = new SimpleCompatibilityChecker(
					tripleMap.getSubject());
			tripleMap.getSubject().setCompChecker(ccs);

			for (PO po : tripleMap.pos) {
				CompatibilityChecker ccp = new SimpleCompatibilityChecker(
						po.getPredicate());
				po.getPredicate().setCompChecker(ccp);
				CompatibilityChecker cco = new SimpleCompatibilityChecker(
						po.getObject());
				po.getObject().setCompChecker(cco);
			}
		}

	}

	private void resolveR2RMLShortcuts() {
		String query = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> INSERT { ?x rr:subjectMap [ rr:constant ?y ]. } WHERE {?x rr:subject ?y.}";
		UpdateExecutionFactory.create(UpdateFactory.create(query),
				GraphStoreFactory.create(reasoningModel)).execute();
		query = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> INSERT { ?x rr:predicateMap [ rr:constant ?y ]. } WHERE {?x rr:predicate ?y.}";
		UpdateExecutionFactory.create(UpdateFactory.create(query),
				GraphStoreFactory.create(reasoningModel)).execute();
		query = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> INSERT { ?x rr:objectMap [ rr:constant ?y ]. } WHERE {?x rr:object ?y.}";
		UpdateExecutionFactory.create(UpdateFactory.create(query),
				GraphStoreFactory.create(reasoningModel)).execute();
		query = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> INSERT { ?x rr:graphMap [ rr:constant ?y ]. } WHERE {?x rr:graph ?y.}";
		UpdateExecutionFactory.create(UpdateFactory.create(query),
				GraphStoreFactory.create(reasoningModel)).execute();
		reasoningModel.size();

	}

	

	

	private int queryCount = 1;

	public Set<TripleMap> getTripleMaps() {

		return new HashSet(tripleMaps.values());
	}

	private void loadTripleMaps() throws R2RMLValidationException,
			JSQLParserException {
		Map<String, TripleMap> tripleMaps = new HashMap<String, TripleMap>();

		String tmquery = "PREFIX rr: <http://www.w3.org/ns/r2rml#> SELECT ?tm ?tableName ?query ?version {?tm a rr:TriplesMap. ?tm rr:logicalTable ?tab . {?tab rr:tableName ?tableName} UNION {?tab rr:sqlQuery ?query. OPTIONAL{?tab rr:sqlVersion ?version}}}";
		ResultSet tmrs = QueryExecutionFactory.create(
				QueryFactory.create(tmquery), reasoningModel).execSelect();

		while (tmrs.hasNext()) {

			QuerySolution solution = tmrs.next();
			Resource tmUri = solution.get("tm").asResource();
			String tablename = solution.get("?tableName") != null ? solution
					.get("?tableName").asLiteral().getString() : null;
			String query = solution.get("?query") != null ? solution
					.get("?query").asLiteral().getString() : null;
			Resource version = solution.get("?version") != null ? solution.get(
					"?version").asResource() : null;

			FromItem fromItem;
			Table fromTable;
			SubSelect subsel;
			if (tablename != null && query == null && version == null) {
				tablename = unescape(tablename);

				fromTable = new Table(null, tablename);
				fromTable.setAlias(tablename);
				fromItem = fromTable;
			} else if (tablename == null && query != null) {
				query = cleanSql(query);
				subsel = new SubSelect();
				subsel.setAlias("query_" + queryCount++);
				subsel.setSelectBody(new SelectBodyString(query));
				fromTable = new Table(null, subsel.getAlias());
				fromTable.setAlias(subsel.getAlias());
				fromItem = subsel;

			} else {
				throw new R2RMLValidationException(
						"Odd virtual table declaration in term map: "
								+ tmUri.toString());
			}

			// validate fromItem

			try {
				dbconf.validateFromItem(fromItem);
			} catch (SQLException e) {
				throw new R2RMLValidationException(
						"Error validation the logical table in mapping "
								+ tmUri.getURI(), e);
			}

			TripleMap triplemap = new TripleMap(tmUri.getURI(), fromItem);

			// get the s-term
			String squery = "PREFIX rr: <http://www.w3.org/ns/r2rml#> SELECT"
					+ " * {" + " <" + tmUri.getURI() + "> rr:subjectMap ?sm  "
					+ getTermMapQuery("s") + "}";

			// "{?sm rr:column ?column} UNION {?sm rr:constant ?constant} UNION {?sm rr:template ?template} OPTIONAL {?sm rr:class ?class} OPTIONAL {?sm rr:termType ?termtype}}"
			// ;
			ResultSet srs = QueryExecutionFactory.create(
					QueryFactory.create(squery), this.mapping).execSelect();
			// there should only be one
			if (srs.hasNext() == false) {
				throw new R2RMLValidationException("Triple map " + tmUri
						+ " has no subject term map, fix this");
			}
			QuerySolution sSoltution = srs.next();
			TermMapQueryResult sres = new TermMapQueryResult(sSoltution, "s",
					fromItem);

			if (srs.hasNext() == true) {
				throw new R2RMLValidationException("Triple map " + tmUri
						+ " has more than one subject term map, fix this");
			}

			// get the s-graph
			String sgraphquery = "PREFIX rr: <http://www.w3.org/ns/r2rml#> SELECT  * {"
					+ " <"
					+ tmUri.getURI()
					+ "> rr:subjectMap ?sm. "
					+ "?sm rr:graphMap ?gm. "
					+ "OPTIONAL { ?gm rr:template ?template } "
					+ "OPTIONAL { ?gm rr:column ?column } "
					+ "OPTIONAL { ?gm rr:constant ?constant } }";

			ResultSet sgraphrs = QueryExecutionFactory.create(
					QueryFactory.create(sgraphquery), this.mapping)
					.execSelect();
			Expression graph = null;
			while (sgraphrs.hasNext()) {
				QuerySolution graphqs = sgraphrs.next();
				if (graphqs.get("?template") != null) {
					String template = graphqs.get("?template").asLiteral()
							.toString();
					graph = columnhelper.getGraphExpression(
							cleanTemplate(template, fromItem), fromItem, dth);
				} else if (graphqs.get("?column") != null) {
					String template = "\"{"
							+ getRealColumnName(graphqs.get("?column")
									.asLiteral().toString(), fromItem) + "\"}";
					graph = columnhelper.getGraphExpression(
							cleanTemplate(template, fromItem), fromItem, dth);
				} else if (graphqs.get("?constant") != null) {
					RDFNode constant = graphqs.get("?constant");
					graph = columnhelper.getGraphExpression(constant, dth);

				}
			}

			// String stemplate =
			// String scolumnName =
			// sSoltution.get("?column")!=null?sSoltution.get("?column").asLiteral().getString():null;
			// RDFNode snode = sSoltution.get("?constant");
			// Resource tmClass = sSoltution.getResource("?class");
			// Resource termType = sSoltution.getResource("?termtype");
			int nodeType;
			if (sres.termType == null || sres.termType.hasURI(R2RML.IRI)) {
				sres.termTypeInt = ColumnHelper.COL_VAL_TYPE_RESOURCE;
			} else {
				sres.termTypeInt = ColumnHelper.COL_VAL_TYPE_BLANK;
			}
			// some validation
			if (sres.termType != null && sres.termType.hasURI(R2RML.Literal)) {
				throw new R2RMLValidationException(
						"no literal in subject position");
			}
			if (sres.constant != null && !sres.constant.isURIResource()) {
				throw new R2RMLValidationException(
						"Must IRI in predicate position");

			}

			triplemap.subject = createTermMap(fromItem, tmUri, triplemap,
					graph, sres, sres.termTypeInt);

			// get the POs

			String poQuery = "PREFIX rr: <http://www.w3.org/ns/r2rml#> "
					+ "SELECT * {"
					+ // ?ptemplate ?pcolumn ?pconstant ?ptermtype ?otemplate
						// ?ocolumn ?oconstant ?otermtype ?olang ?odatatype {" +
					"<" + tmUri.getURI() + "> <" + R2RML.predicateObjectMap
					+ "> ?pom. ?pom <" + R2RML.predicateMap + "> ?pm . ?pom <"
					+ R2RML.objectMap + "> ?om." + getTermMapQuery("p")
					+ getTermMapQuery("o") + "}";

			ResultSet pors = QueryExecutionFactory.create(
					QueryFactory.create(poQuery), reasoningModel).execSelect();

			while (pors.hasNext()) {
				QuerySolution posol = pors.next();

				TermMapQueryResult p = new TermMapQueryResult(posol, "p",
						fromItem);
				TermMap ptm = null;

				// some general validation
				if (p.termType != null
						&& !p.termType.getURI().equals(R2RML.IRI)) {
					throw new R2RMLValidationException(
							"Only use iris in predicate position");
				}

				ptm = createTermMap(fromItem, tmUri, triplemap, graph, p,
						ColumnHelper.COL_VAL_TYPE_RESOURCE);

				TermMapQueryResult o = new TermMapQueryResult(posol, "o",
						fromItem);
				o.termTypeInt = ColumnHelper.COL_VAL_TYPE_RESOURCE;

				TermMap otm = null;

				if (o.column != null
						|| o.lang != null
						|| o.datatypeuri != null
						|| (o.termType != null && o.termType.getURI().equals(
								R2RML.Literal))) {
					o.termTypeInt = ColumnHelper.COL_VAL_TYPE_LITERAL;
				} else if (o.termType != null
						&& o.termType.getURI().equals(R2RML.BlankNode)) {
					o.termTypeInt = ColumnHelper.COL_VAL_TYPE_BLANK;
				}

				otm = createTermMap(fromItem, tmUri, triplemap, graph, o,
						o.termTypeInt);

				triplemap.addPO(ptm, otm);

			}
			
			tripleMaps.put(tmUri.toString(), triplemap);

		}

		this.tripleMaps = tripleMaps;
	}

	private TermMap createTermMap(FromItem fromItem, Resource tmUri,
			TripleMap triplemap, Expression graph, TermMapQueryResult tmqrs,
			Integer otermtype) {
		TermMap tm = null;
		String datatype = tmqrs.datatypeuri != null ? tmqrs.datatypeuri
				.getURI() : null;
		if (tmqrs.column != null) {
			// generate from colum
			Column col = new Column();
			if (fromItem instanceof Table) {
				col.setTable((Table) fromItem);
			} else {
				Table tab = new Table("null", fromItem.getAlias());
				tab.setAlias(fromItem.getAlias());
				col.setTable(tab);
			}

			col.setColumnName(tmqrs.column);
			List<Expression> oexprs = columnhelper.getExpression(col,
					otermtype, dbconf.getDataType(fromItem, tmqrs.column),
					datatype, tmqrs.lang, null, dth, graph,
					tmUri.getNameSpace());
			tm = new TermMap(dth, oexprs, Arrays.asList(fromItem), null,
					triplemap);
		} else if (tmqrs.constant != null) {
			// use constant term

			List<Expression> oexprs = columnhelper.getExpression(
					tmqrs.constant, dth, graph);
			tm = new TermMap(dth, oexprs, Arrays.asList(fromItem), null,
					triplemap);
		} else if (tmqrs.template != null) {
			int sqlType = ColumnHelper.COL_VAL_SQL_TYPE_RESOURCE;
			if (!otermtype.equals(ColumnHelper.COL_VAL_RES_LENGTH_LITERAL)) {
				sqlType = Types.VARCHAR;
			}

			// from template
			List<Expression> otmExpressions = columnhelper.getExpression(
					tmqrs.template, otermtype, sqlType, datatype, tmqrs.lang,
					null, dth, fromItem, graph, tmUri.getNameSpace());
			tm = new TermMap(dth, otmExpressions, Arrays.asList(fromItem),
					null, triplemap);
		}
		return tm;
	}

	/**
	 * creates a part of a query that creates
	 * 
	 * @param prefix
	 * @return
	 */
	private String getTermMapQuery(String prefix) {
		String p = prefix;
		String query = "{?" + p + "m rr:column ?" + p + "column} " + "UNION {?"
				+ p + "m rr:constant ?" + p + "constant} " + "UNION {?" + p
				+ "m rr:template ?" + p + "template} " + "OPTIONAL {?" + p
				+ "m rr:termType ?" + p + "termtype} " + "OPTIONAL {?" + p
				+ "m rr:datatype ?" + p + "datatype} " + "OPTIONAL {?" + p
				+ "m <" + R2RML.language + "> ?" + p + "lang} " + "OPTIONAL {?"
				+ p + "m <" + R2RML.inverseExpression + "> ?" + p
				+ "inverseexpression}";

		return query;
	}

	private class TermMapQueryResult {

		public TermMapQueryResult(QuerySolution sol, String prefix, FromItem fi) {
			template = sol.get("?" + prefix + "template") != null ? cleanTemplate(
					sol.get("?" + prefix + "template").asLiteral().getString(),
					fi) : null;
			column = sol.get("?" + prefix + "column") != null ? sol
					.get("?" + prefix + "column").asLiteral().getString()
					: null;
			column = getRealColumnName(column, fi);

			lang = sol.get("?" + prefix + "lang") != null ? sol
					.get("?" + prefix + "lang").asLiteral().getString() : null;
			inverseExpression = sol.get("?" + prefix + "inverseexpression") != null ? sol
					.get("?" + prefix + "inverseexpression").asLiteral()
					.getString()
					: null;
			constant = sol.get("?" + prefix + "constant");
			datatypeuri = sol.get("?" + prefix + "datatype") != null ? sol.get(
					"?" + prefix + "datatype").asResource() : null;
			tmclass = sol.get("?" + prefix + "tmclass") != null ? sol.get(
					"?" + prefix + "tmclass").asResource() : null;
			termType = sol.getResource("?" + prefix + "termtype");
		}

		String[] template;
		String column;
		RDFNode constant;
		String lang;
		Resource datatypeuri;
		String inverseExpression;
		Resource tmclass;
		Resource termType;
		int termTypeInt;
	}

	public String getRealColumnName(String unrealColumnName, FromItem fi) {
		if (unrealColumnName == null) {
			return null;
		} else if (unrealColumnName.startsWith("\"")
				&& unrealColumnName.endsWith("\"")) {
			return unrealColumnName.substring(1, unrealColumnName.length() - 1);
		} else {
			// not escaped, so we need to see how the database handles the
			// string.
			return dbconf.getColumnName(fi, unrealColumnName);
		}
	}

	public static String unescape(String toUnescape) {
		if (toUnescape != null && toUnescape.startsWith("\"")
				&& toUnescape.endsWith("\"")) {
			return toUnescape.substring(1, toUnescape.length() - 1);
		} else {
			// not escaped, so we need to see how the database handles the
			// string.
			return toUnescape;
		}
	}

	/**
	 * removes all apostrophes from the template and ensure they are correctly
	 * capitalized.
	 * 
	 * @return
	 */

	public String[] cleanTemplate(String template, FromItem fi) {

		// ((?<!\\\\)\\{)|(\\})
		List<String> altSeq = Arrays.asList(template
				.split("((?<!\\\\)\\{)|(?<!\\\\)\\}"));
		List<String> cleaned = new ArrayList<String>();

		for (int i = 0; i < altSeq.size(); i++) {
			if (i % 2 == 1) {

				cleaned.add(getRealColumnName(altSeq.get(i), fi));

			} else {
				// static part, no need to change anything, just remove the
				// escape patterns;

				cleaned.add(altSeq.get(i).replaceAll("\\\\", ""));

			}
		}

		return cleaned.toArray(new String[0]);
	}

	public String cleanSql(String toUnescape) {
		if (toUnescape != null) {

			toUnescape = toUnescape.trim();
			toUnescape = toUnescape.replaceAll("\r\n", " ").replaceAll("\n",
					" ");
			if (toUnescape.endsWith(";")) {
				toUnescape = toUnescape.substring(0, toUnescape.length() - 1);
			}

			return toUnescape;
		} else {
			return toUnescape;
		}
	}

	public RDFDatatype getSqlDataType(String tablename, String colname) {
		throw new ImplementationException("implement sql type table");
	}
	
	
	
	//this method analyzes the r2rml file for the most common errors
	public boolean validate(){
		
		
		
		//check if the uri starting with the r2rml prefix are all actually in r2rml
		
//		 StmtIterator stit = mapping.listStatements();
//		 
//		 while(stit.hasNext()){
//			 Statement st = stit.next();
//			 if(st.getSubject().toString().startsWith(R2RML.R2RML)){
//				 this.r2rmlSchema.conta
//			 }
//		 }
//		
//		
		
		
		
		boolean isValid = true;
		
		//do we have at least one triples map?
		List<Resource> triplesMaps = reasoningModel.listResourcesWithProperty(RDF.type, ResourceFactory.createResource(R2RML.TriplesMap)).toList();
		if(triplesMaps.isEmpty()){
			log.error("No triples maps found in this configuration file. Please check, if this is the correct file. Otherwise make sure that at least one triples map is in the file.");
			isValid = false;
		}else{
			//does every triple map have exactly one valid logical table declaration?
			for (Resource tripleMap : triplesMaps) {
				List<RDFNode> logicalTables = reasoningModel.listObjectsOfProperty(tripleMap, ResourceFactory.createProperty(R2RML.logicalTable)).toList();
				if(logicalTables.isEmpty()){
					throw new R2RMLValidationException("No rr:logicalTable property found for triples map " + tripleMap.getURI());
				}
				for (RDFNode logicalTableNode : logicalTables) {
					if(logicalTableNode.isLiteral()){
						isValid = false;

						throw new R2RMLValidationException("Error in triples map" + tripleMap.getURI() + " rr:logicalTable has a string object. Please use an intermediate node with rr:tableName or rr:sqlQuery.");
					}else{
						Resource logicalTable = logicalTableNode.asResource();
						List<RDFNode> tableNames = reasoningModel.listObjectsOfProperty(logicalTable, ResourceFactory.createProperty(R2RML.tableName)).toList();
						for (RDFNode tableName : tableNames) {
							if(!tableName.isLiteral()){
								isValid = false;

								throw new R2RMLValidationException("tablename of triple map " +tripleMap+ " is not a literal.");
							}
						}
						List<RDFNode> queries = reasoningModel.listObjectsOfProperty(logicalTable, ResourceFactory.createProperty(R2RML.sqlQuery)).toList();
						for (RDFNode query : queries) {
							if(!query.isLiteral()){
								isValid = false;

								throw new R2RMLValidationException("query of triple map " +tripleMap+ " is not a literal.");
							}
						}
						
						if(tableNames.size() + queries.size()==0){
							throw new R2RMLValidationException("No table name or query is given for triple map " +  tripleMap.getURI());
						}
						if(tableNames.size() + queries.size()>1){
							throw new R2RMLValidationException("Multiple table names or queries are given for triple map " +  tripleMap.getURI());
						}
					}
					
				}
				
				//now checking for the subject map.
				
				List<RDFNode> subjectMaps = reasoningModel.listObjectsOfProperty(tripleMap, ResourceFactory.createProperty(R2RML.subjectMap)).toList();
				
								
				
				
				
				
				//now checking for the predicateObject maps.
				
				List<RDFNode> poMaps = reasoningModel.listObjectsOfProperty(tripleMap, ResourceFactory.createProperty(R2RML.predicateObjectMap)).toList();
				if(poMaps.size()==0){
					throw new R2RMLValidationException("No Predicate-Object Maps given for triple map:" + tripleMap.getURI());
				}
				
				for (RDFNode pomap : poMaps) {
					List<RDFNode> predicatemaps =  reasoningModel.listObjectsOfProperty(pomap.asResource(),  ResourceFactory.createProperty(R2RML.predicateMap)).toList();
					if(predicatemaps.size()!=1){
						throw new R2RMLValidationException("Found predicateObjectmap without an predicate in triple map: " +  tripleMap.getURI() );
					}
					if(!(predicatemaps.get(0).asResource().hasProperty(ResourceFactory.createProperty(R2RML.template))
						||predicatemaps.get(0).asResource().hasProperty(ResourceFactory.createProperty(R2RML.constant))
						||predicatemaps.get(0).asResource().hasProperty(ResourceFactory.createProperty(R2RML.column)))){
						throw new R2RMLValidationException("predicate defintion not valid in triples map " + tripleMap.getURI());
					}
					
					
					
					
					
					
					List<RDFNode> objectmaps =  reasoningModel.listObjectsOfProperty(pomap.asResource(),  ResourceFactory.createProperty(R2RML.objectMap)).toList();
					if(objectmaps.size()<1){
						throw new R2RMLValidationException("Found predicateObjectmap without an object in triple map: " +  tripleMap.getURI() );
					}
					if(!(objectmaps.get(0).asResource().hasProperty(ResourceFactory.createProperty(R2RML.template))
							||objectmaps.get(0).asResource().hasProperty(ResourceFactory.createProperty(R2RML.constant))
							||objectmaps.get(0).asResource().hasProperty(ResourceFactory.createProperty(R2RML.parentTriplesMap))
							||objectmaps.get(0).asResource().hasProperty(ResourceFactory.createProperty(R2RML.column))
							||(objectmaps.size()>1
							 && objectmaps.get(1).asResource().hasProperty(ResourceFactory.createProperty(R2RML.parentTriplesMap)))
							)){
							throw new R2RMLValidationException("object defintion not valid in triples map " + tripleMap.getURI());
						}
					
					List<RDFNode> parentTripleMaps = reasoningModel.listObjectsOfProperty(objectmaps.get(0).asResource(), ResourceFactory.createProperty(R2RML.parentTriplesMap)).toList();
					if(parentTripleMaps.size()>1){
						if(!parentTripleMaps.get(0).asResource().hasProperty(ResourceFactory.createProperty(R2RML.logicalTable))){
							throw new R2RMLValidationException("Triples map " + parentTripleMaps.get(0)+ " is used as parent triples in " + tripleMap.getURI() + " but the referenced resource does not have a rr:logicalTable");
						}
					}
					
				}
				
				
				
				
			}
			
			
			
		}
		
		
	
		
		
		
 
		
	
		return isValid;
		
	}
	
	
	public void validatepost(){
		// in the end every triple map should have predicate objects.
		for(TripleMap triplemap : this.tripleMaps.values()){
			if(triplemap.pos.size()==0){
				throw new R2RMLValidationException("Make sure there are predicate-object maps in triple map: " + triplemap.getUri() );
			}
		}
	}
	
}
