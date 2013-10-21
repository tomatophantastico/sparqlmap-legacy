package org.aksw.sparqlmap.core.config.syntax.r2rml;

import java.sql.Date;
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
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SelectBodyString;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap.PO;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.mapper.compatibility.CompatibilityChecker;
import org.aksw.sparqlmap.core.mapper.compatibility.SimpleCompatibilityChecker;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.core.mapper.translate.FilterUtil;
import org.aksw.sparqlmap.core.mapper.translate.ImplementationException;
import org.springframework.beans.factory.annotation.Autowired;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.update.GraphStoreFactory;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

public class R2RMLModel {

	public R2RMLModel(DBAccess dbconf,
			DataTypeHelper dth, Model mapping, Model r2rmlSchema) {
		super();
		
		this.dbconf = dbconf;
		this.dth = dth;
		this.mapping = mapping;
		this.r2rmlSchema = r2rmlSchema;
	}
	
	@Autowired
	private TermMapFactory tfac;

	
	private DBAccess dbconf;
	private DataTypeHelper dth;
	Model mapping = null;
	Model r2rmlSchema = null;
	

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
		resolveMultipleGraphs();
		validate();
		
		loadTripleMaps();
		loadParentTripleStatements();

		loadCompatibilityChecker();
		validatepost();
	}

	private void resolveMultipleGraphs() {
		
		//for all triple maps with multiple graph statements we first get all the subject triple maps and put them into the po maps
		
		List<Resource> allTripleMaps = reasoningModel.listSubjectsWithProperty(RDF.type, R2RML.TriplesMap).toList();
		
		for(Resource tripleMap : allTripleMaps){
			
			//get the subject, we assume that the r2rml is valid and therefore has only one subject.
			Resource subject = reasoningModel.listObjectsOfProperty(tripleMap, R2RML.subjectMap).next().asResource();
			
			//get the graph resource
			List<RDFNode> subjectGraphMaps = reasoningModel.listObjectsOfProperty(subject, R2RML.graphMap).toList();
					
			//for all these graph statements
			for(RDFNode graph: subjectGraphMaps){
				for(RDFNode po: reasoningModel.listObjectsOfProperty(tripleMap,R2RML.predicateObjectMap).toList()){
					//we add the the graph map into the PO map
					reasoningModel.add(po.asResource(),R2RML.graphMap,graph);
				}
			}
			
			// and remove them from the mapping
			for (RDFNode graph : subjectGraphMaps) {
				reasoningModel.remove(subject,R2RML.graphMap,graph);
			}
		}
		
		
	}

	private void loadParentTripleStatements() {
		
		
		
		
		
		 List<Statement> parentTripleMapStatements = reasoningModel.listStatements((Resource)null, R2RML.parentTriplesMap, (RDFNode) null).toList();
		
		 
		 for (Statement statement : parentTripleMapStatements) {
			 
			 Resource objectMap = statement.getSubject();
			 
			 //get the referenced map
			 Resource parentTripleMap = statement.getObject().asResource();
			 TripleMap parentTrM = this.tripleMaps.get(parentTripleMap.getURI());

			 
			 //get the child map
			 Resource poMap = reasoningModel.listStatements(null, R2RML.objectMap, objectMap).toList().get(0).getSubject();
			 Resource mapping = reasoningModel.listStatements(null, R2RML.predicateObjectMap, poMap).toList().get(0).getSubject();
			 TripleMap tripleMap = this.tripleMaps.get(mapping.getURI());

			 //we insert this
			 TermMap newoTermMap = parentTrM.getSubject().clone("");
			 
			 newoTermMap.trm = parentTrM;

			 
			 
			 //get the join condition
			 List<Statement> joinconditions =  reasoningModel.listStatements(objectMap, R2RML.joinCondition,(RDFNode) null).toList();
			 for (Statement joincondition : joinconditions) {
				Resource joinconditionObject = joincondition.getObject().asResource();
				
				String parentjc = unescape(reasoningModel.listObjectsOfProperty(joinconditionObject, R2RML.parent).toList().get(0).asLiteral().getString());
				String childjc = unescape(reasoningModel.listObjectsOfProperty(joinconditionObject, R2RML.child).toList().get(0).asLiteral().getString());
				
				
				this.dbconf.getDataType(parentTrM.from,
						getRealColumnName(parentjc, parentTrM.from));
			
				Column leftCol = ColumnHelper.createColumn(parentTrM.from.getAlias(), parentjc);
				
				Column rightCol = ColumnHelper.createColumn(tripleMap.from.getAlias(), childjc);
				EqualsTo eq = new EqualsTo();
				eq.setLeftExpression(dth.cast(leftCol, dth.getStringCastType()));
				eq.setRightExpression(dth.cast(rightCol,
						dth.getStringCastType()));
				newoTermMap.getFromJoins().add(eq);
				newoTermMap.addFromItem(tripleMap.from);

				log.debug("And joins on parent: " + parentTrM.from.toString()
						+ "." + parentjc + " and " + tripleMap.from.toString()
						+ "." + childjc);
				
			}
			 
			 
			 
			// now we need to create the predicate
			for (RDFNode pnode : reasoningModel.listObjectsOfProperty(poMap,
					R2RML.predicateMap).toList()) {
				// get the predicate Map
				TermMapQueryResult ptmqr = new TermMapQueryResult(pnode.asResource(),
						reasoningModel, tripleMap.from);
				
				TermMap ptm = mapQueryResultOnTermMap(ptmqr, tripleMap.from,tripleMap);
				
				//just to make sure
				ptm.setTermTyp(R2RML.IRI);
				

				tripleMap.addPO(ptm, newoTermMap);

			}

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
					tripleMap.getSubject(),this.dbconf,this.dth);
			tripleMap.getSubject().setCompChecker(ccs);

			for (PO po : tripleMap.getPos()) {
				CompatibilityChecker ccp = new SimpleCompatibilityChecker(
						po.getPredicate(),this.dbconf,this.dth);
				po.getPredicate().setCompChecker(ccp);
				CompatibilityChecker cco = new SimpleCompatibilityChecker(
						po.getObject(),this.dbconf,this.dth);
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
	private int tableCount = 1;

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
				log.info("Table named " + tablename + " is referred to as: " +tableCount++);
				fromTable.setAlias("table_" + tableCount);
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
			List<Expression> graph = null;
			while (sgraphrs.hasNext()) {
				QuerySolution graphqs = sgraphrs.next();
				if (graphqs.get("?template") != null) {
					String template = graphqs.get("?template").asLiteral()
							.toString();
					graph = templateToResourceExpression(
							cleanTemplate(template, fromItem), fromItem, dth);
				} else if (graphqs.get("?column") != null) {
					String template = "\"{"
							+ getRealColumnName(graphqs.get("?column")
									.asLiteral().toString(), fromItem) + "\"}";
					graph = templateToResourceExpression(
							cleanTemplate(template, fromItem), fromItem, dth);
				} else if (graphqs.get("?constant") != null) {
					RDFNode constant = graphqs.get("?constant");
					graph = Arrays.asList( resourceToExpression(constant.asResource()));

				}
			}
			
			if(graph==null){
				graph = Arrays.asList( resourceToExpression(R2RML.defaultGraph));
			}
			
			TermMap stm =mapQueryResultOnTermMap(sres, fromItem,triplemap);

			if (sres.termType != null) {
				stm.setTermTyp(sres.termType);
			} else {
				stm.setTermTyp(R2RML.IRI);
			}
			// some validation
			if (sres.termType != null && sres.termType.equals(R2RML.Literal)) {
				throw new R2RMLValidationException(
						"no literal in subject position");
			}
			if (sres.constant != null && !sres.constant.isURIResource()) {
				throw new R2RMLValidationException(
						"Must IRI in predicate position");

			}
			
			stm.trm = triplemap;
			triplemap.setSubject(stm);
			
			//set the graph
			
			TermMap gtm = new TermMap(dth);
			gtm.setTermTyp(R2RML.IRI);
			gtm.getResourceColSeg().addAll(graph);
			
			// get the POsa

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
				TermMap ptm = mapQueryResultOnTermMap(p, fromItem,triplemap);
				// some general validation
				if (p.termType != null
						&& !p.termType.getURI().equals(R2RML.IRI)) {
					throw new R2RMLValidationException(
							"Only use iris in predicate position");
				}
				ptm.setTermTyp(R2RML.IRI);
				
				
				
				

				TermMapQueryResult qr_o = new TermMapQueryResult(posol, "o",
						fromItem);
				//the term type definition according to the R2RML spec  http://www.w3.org/TR/r2rml/#termtype
				
				TermMap otm = mapQueryResultOnTermMap(qr_o, fromItem,triplemap);
			
				//Identify the term type here
				if(qr_o.termType != null){
					otm.setTermTyp(qr_o.termType);	
				}else if(qr_o.constant!=null){
					//use the termtype of the constant
					if(qr_o.constant.isAnon()){
						otm.setTermTyp(R2RML.BlankNode);
					}else if(qr_o.constant.isURIResource()){
						otm.setTermTyp(R2RML.IRI);
					}else{
						otm.setTermTyp(R2RML.Literal);
					}
				}else if(qr_o.column != null //when column, etc. then it is a literal
							|| qr_o.lang != null
							|| qr_o.datatypeuri != null
							|| (qr_o.termType != null && qr_o.termType.equals(
									R2RML.Literal))){
						otm.setTermTyp(R2RML.Literal);
					}else{
						//it stays IRI
						otm.setTermTyp(R2RML.IRI);
					}
				
				
				triplemap.addPO(ptm, otm);

			}
			
			tripleMaps.put(tmUri.toString(), triplemap);

		}

		this.tripleMaps = tripleMaps;
	}
//
//	private TermMap createTermMap(FromItem fromItem, Resource tmUri,
//			TripleMap triplemap, Expression graph, TermMapQueryResult tmqrs,
//			Integer otermtype) {
//		TermMap tm = null;
//		
//		
//		String datatype = tmqrs.datatypeuri != null ? tmqrs.datatypeuri
//				.getURI() : null;
//		if (tmqrs.column != null) {
//			// generate from colum
//			Column col = new Column();
//			if (fromItem instanceof Table) {
//				col.setTable((Table) fromItem);
//			} else {
//				Table tab = new Table("null", fromItem.getAlias());
//				tab.setAlias(fromItem.getAlias());
//				col.setTable(tab);
//			}
//
//			col.setColumnName(tmqrs.column);
//			List<Expression> oexprs = columnhelper.getExpression(col,
//					otermtype, dbconf.getDataType(fromItem, tmqrs.column),
//					datatype, tmqrs.lang, null, dth);
//			tm = TermMap.createTermMap(dth, oexprs, Arrays.asList(fromItem), null,
//					triplemap);
//		} else if (tmqrs.constant != null) {
//			// use constant term
//
//			List<Expression> oexprs = columnhelper.getExpression(
//					tmqrs.constant.asNode(), dth);
//			tm = TermMap.createTermMap(dth, oexprs, Arrays.asList(fromItem), null,
//					triplemap);
//		} else if (tmqrs.template != null) {
//			int sqlType = ColumnHelper.COL_VAL_SQL_TYPE_RESOURCE;
//			if (!otermtype.equals(ColumnHelper.COL_VAL_RES_LENGTH_LITERAL)) {
//				sqlType = Types.VARCHAR;
//			}
//
//			// from template
//			List<Expression> otmExpressions = columnhelper.getExpression(
//					tmqrs.template, otermtype, sqlType, datatype, tmqrs.lang,
//					null, dth, fromItem, graph, tmUri.getNameSpace());
//			tm = TermMap.createTermMap(dth, otmExpressions, Arrays.asList(fromItem),
//					null, triplemap);
//		}
//		return tm;
//	}

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
		
		
		
		public TermMapQueryResult(Resource tm, Model model, FromItem fi){
			template = model.listObjectsOfProperty(tm, R2RML.template).hasNext()?cleanTemplate(model.listObjectsOfProperty(tm, R2RML.template).next().asLiteral().getString(),fi):null;
			
			column = model.listObjectsOfProperty(tm, R2RML.column).hasNext()?getRealColumnName(model.listObjectsOfProperty(tm, R2RML.column).next().asLiteral().getString(), fi):null;
			
			lang = model.listObjectsOfProperty(tm, R2RML.language).hasNext()?model.listObjectsOfProperty(tm, R2RML.language).next().asLiteral().getString():null;
			
			inverseExpression = model.listObjectsOfProperty(tm, R2RML.inverseExpression).hasNext()?model.listObjectsOfProperty(tm, R2RML.inverseExpression).next().asLiteral().getString():null;
			constant = model.listObjectsOfProperty(tm, R2RML.constant).hasNext()?model.listObjectsOfProperty(tm, R2RML.constant).next():null;
			datatypeuri =model.listObjectsOfProperty(tm, R2RML.datatype).hasNext()?model.listObjectsOfProperty(tm, R2RML.datatype).next().asResource():null;
			
			tmclass = model.listObjectsOfProperty(tm, R2RML.hasClass).hasNext()?model.listObjectsOfProperty(tm, R2RML.hasClass).next().asResource():null;
			termType =  model.listObjectsOfProperty(tm, R2RML.termType).hasNext()?model.listObjectsOfProperty(tm, R2RML.termType).next().asResource():null;
			
			
		}

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
	
	public TermMap mapQueryResultOnTermMap(TermMapQueryResult qr, FromItem fi, TripleMap tripleMap){
		
		TermMap tm = null;
		
		
		
		if(qr.constant!=null){
			
			tm = tfac.createTermMap(qr.constant.asNode());
			
			
			
		}else if(qr.template!=null){
			if(tm.getTermType().equals(R2RML.Literal)){
				
				List<Expression> resourceExpression = templateToResourceExpression(qr.template, fi, dth);
				
				tm.literalValString = FilterUtil.concat(resourceExpression.toArray(new Expression[0]));

			}else{
				List<Expression> resourceExpression = templateToResourceExpression(qr.template, fi, dth);
				tm.getResourceColSeg().addAll(resourceExpression);
			}
			
		}else if(qr.column!=null){
			
			Column col = ColumnHelper.createCol(fi.getAlias(), qr.column);
			
			if(tm.getTermType().equals(R2RML.Literal)){
				
				int sqlType = dbconf.getDataType(fi, qr.column);
						
				if (tm.literalType == null && DataTypeHelper.getRDFDataType(sqlType) != null) {
					tm.setLiteralDataType(DataTypeHelper.getRDFDataType(sqlType).getURI());	
				}
				RDFDatatype dt = DataTypeHelper.getRDFDataType(sqlType);
				if(dt==null){
					tm.setLiteralDataType(RDFS.Literal.getURI());
				}else{
					tm.setLiteralDataType(dt.getURI());
				}
				
				
				
				if(dth.getCastTypeString(dt).equals(dth.getStringCastType())){
					tm.literalValString = dth.cast(col, dth.getStringCastType());
					
				}else if(dth.getCastTypeString(dt).equals(dth.getNumericCastType())){
					tm.literalValNumeric = dth.cast(col, dth.getNumericCastType());
					
				}else if(dth.getCastTypeString(dt).equals(dth.getBinaryDataType())){
					tm.literalValBinary = dth.cast(col, dth.getBinaryDataType());
					
				}else if(dth.getCastTypeString(dt).equals(dth.getDateCastType())){
					tm.literalValDate = dth.cast(col, dth.getDateCastType());
					
				}else if(dth.getCastTypeString(dt).equals(dth.getBooleanCastType())){
					tm.literalValBool = dth.cast(col, dth.getBooleanCastType());
				}

			}else{
				tm.resourceColSeg.add(dth.castNull(dth.getStringCastType()));
				tm.resourceColSeg.add(dth.cast(col,dth.getStringCastType()));
			}		
		}
		
		
		if(!tm.isConstant()){
			tm.alias2fromItem.put(fi.getAlias(), fi);
		}
		tm.trm = tripleMap;
		return tm;
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
		
		boolean isValid = true;
		
		//do we have at least one triples map?
		List<Resource> triplesMaps = reasoningModel.listResourcesWithProperty(RDF.type,R2RML.TriplesMap).toList();
		if(triplesMaps.isEmpty()){
			log.error("No triples maps found in this configuration file. Please check, if this is the correct file. Otherwise make sure that at least one triples map is in the file.");
			isValid = false;
		}else{
			//does every triple map have exactly one valid logical table declaration?
			for (Resource tripleMap : triplesMaps) {
				List<RDFNode> logicalTables = reasoningModel.listObjectsOfProperty(tripleMap, R2RML.logicalTable).toList();
				if(logicalTables.isEmpty()){
					throw new R2RMLValidationException("No rr:logicalTable property found for triples map " + tripleMap.getURI());
				}
				for (RDFNode logicalTableNode : logicalTables) {
					if(logicalTableNode.isLiteral()){
						isValid = false;

						throw new R2RMLValidationException("Error in triples map" + tripleMap.getURI() + " rr:logicalTable has a string object. Please use an intermediate node with rr:tableName or rr:sqlQuery.");
					}else{
						Resource logicalTable = logicalTableNode.asResource();
						List<RDFNode> tableNames = reasoningModel.listObjectsOfProperty(logicalTable, R2RML.tableName).toList();
						for (RDFNode tableName : tableNames) {
							if(!tableName.isLiteral()){
								isValid = false;

								throw new R2RMLValidationException("tablename of triple map " +tripleMap+ " is not a literal.");
							}
						}
						List<RDFNode> queries = reasoningModel.listObjectsOfProperty(logicalTable, R2RML.sqlQuery).toList();
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
				
				List<RDFNode> subjectMaps = reasoningModel.listObjectsOfProperty(tripleMap,R2RML.subjectMap).toList();
				
								
				//now checking for the predicateObject maps.
				
				List<RDFNode> poMaps = reasoningModel.listObjectsOfProperty(tripleMap,R2RML.predicateObjectMap).toList();
				if(poMaps.size()==0){
					throw new R2RMLValidationException("No Predicate-Object Maps given for triple map:" + tripleMap.getURI());
				}
				
				for (RDFNode pomap : poMaps) {
					List<RDFNode> predicatemaps =  reasoningModel.listObjectsOfProperty(pomap.asResource(),R2RML.predicateMap).toList();
					if(predicatemaps.size()<1){
						throw new R2RMLValidationException("Found predicateObjectmap without an predicate in triple map: " +  tripleMap.getURI() );
					}
					if(!(predicatemaps.get(0).asResource().hasProperty(R2RML.template)
						||predicatemaps.get(0).asResource().hasProperty(R2RML.constant)
						||predicatemaps.get(0).asResource().hasProperty(R2RML.column))){
						throw new R2RMLValidationException("predicate defintion not valid in triples map " + tripleMap.getURI());
					}

					List<RDFNode> objectmaps =  reasoningModel.listObjectsOfProperty(pomap.asResource(),  R2RML.objectMap).toList();
					if(objectmaps.size()<1){
						throw new R2RMLValidationException("Found predicateObjectmap without an object in triple map: " +  tripleMap.getURI() );
					}
					if(!(objectmaps.get(0).asResource().hasProperty(R2RML.template)
							||objectmaps.get(0).asResource().hasProperty(R2RML.constant)
							||objectmaps.get(0).asResource().hasProperty(R2RML.parentTriplesMap)
							||objectmaps.get(0).asResource().hasProperty(R2RML.column)
							||(objectmaps.size()>1
							 && objectmaps.get(1).asResource().hasProperty(R2RML.parentTriplesMap))
							)){
							throw new R2RMLValidationException("object defintion not valid in triples map " + tripleMap.getURI());
						}
					
					List<RDFNode> parentTripleMaps = reasoningModel.listObjectsOfProperty(objectmaps.get(0).asResource(), R2RML.parentTriplesMap).toList();
					if(parentTripleMaps.size()>1){
						if(!parentTripleMaps.get(0).asResource().hasProperty(R2RML.logicalTable)){
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
			if(triplemap.getPos().size()==0){
				throw new R2RMLValidationException("Make sure there are predicate-object maps in triple map: " + triplemap.getUri() );
			}
		}
	}
	
	
	public Expression resourceToExpression(Resource res){
			return dth.cast(new StringValue("\"" + res.getURI() + "\""), dth.getStringCastType());	
	}
	
	public List<Expression> templateToResourceExpression(String[] template,
			FromItem fi, DataTypeHelper dth) {

		List<String> altSeq = Arrays.asList(template);
		List<Expression> newExprs = new ArrayList<Expression>();

		// now create a big concat statement.
		for (int i = 0; i < altSeq.size(); i++) {
			if (i % 2 == 1) {
				String colName = R2RMLModel.unescape(altSeq.get(i));
				// validate and register the colname first
				// dbaccess.getDataType(fi,colName);
				newExprs.add(dth.cast(
						ColumnHelper.createCol(fi.getAlias(), colName),
						dth.getStringCastType()));
			} else {
				newExprs.add(dth.cast(new StringValue("\"" + altSeq.get(i)
						+ "\""), dth.getStringCastType()));
			}
		}
		return newExprs;

	}
	
}
