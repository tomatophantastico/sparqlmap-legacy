package org.aksw.sparqlmap.core.config.syntax.r2rml;

import java.io.StringReader;
import java.sql.SQLException;
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
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectBodyString;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.util.BaseSelectVisitor;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.config.syntax.r2rml.TripleMap.PO;
import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.mapper.compatibility.CompatibilityChecker;
import org.aksw.sparqlmap.core.mapper.compatibility.SimpleCompatibilityChecker;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.core.mapper.translate.FilterUtil;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.update.GraphStoreFactory;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

public class R2RMLModel {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(R2RMLModel.class);
	
	
	@Autowired
	private TermMapFactory tfac;

	private DBAccess dbconf;
	private DataTypeHelper dth;
	Model mapping = null;
	Model r2rmlSchema = null;
	Model reasoningModel = null;
	Multimap<String,TripleMap> tripleMaps = HashMultimap.create();
	
	private int queryCount = 1;
	private int tableCount = 1;

	
	public R2RMLModel(DBAccess dbconf,
			DataTypeHelper dth, Model mapping, Model r2rmlSchema) {
		super();
		
		this.dbconf = dbconf;
		this.dth = dth;
		this.mapping = mapping;
		this.r2rmlSchema = r2rmlSchema;
	}
	
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
		
		decomposeVirtualTableQueries();

		loadCompatibilityChecker();
		validatepost();
	}
	


	

	/**
	 * if a triple map s based on a query, we attempt to decompose it.
	 */
	private void decomposeVirtualTableQueries() {
		TERMMAPLOOP: for(TripleMap trm: tripleMaps.values()){
			FromItem fi = trm.getFrom();
			if(fi instanceof SubSelect){
				SelectBody sb  = ((SubSelect) fi).getSelectBody();
				if(sb instanceof SelectBodyString){
					String queryString = ((SelectBodyString) sb).getQuery();
					CCJSqlParser sqlParser = new CCJSqlParser(new StringReader(queryString));
					try {
						sb = sqlParser.SelectBody();
					} catch (ParseException e) {
						log.warn("Could not parse query for optimization " + queryString);
						continue TERMMAPLOOP;
					}		
				}
				
				if(sb instanceof PlainSelect){
					cleanColumnNames((PlainSelect) sb);
					//validate that there are only normal joins on tables are here
					List<Table> tables = new ArrayList<Table>();
					List<EqualsTo> joinConds = new ArrayList<EqualsTo>();
					
					if(!(((PlainSelect) sb).getFromItem() instanceof Table)){
						continue;
					}
					tables.add((Table) ((PlainSelect) sb).getFromItem());
					
					if (((PlainSelect) sb).getJoins() != null) {
						for (Join join : ((PlainSelect) sb).getJoins()) {
							if ((join.isSimple()) || join.isFull()
									|| join.isLeft()
									|| !(join.getRightItem() instanceof Table)) {
								log.warn("Only simple joins can be opzimized");
								continue TERMMAPLOOP;
							}

							Table tab = (Table) join.getRightItem();
							if (tab.getAlias() == null) {
								log.warn("Table: "
										+ tab.getName()
										+ " needs an alias in order to be optimized");
								continue TERMMAPLOOP;
							}

							tables.add(tab);

							// check if we can make use of the on condition.

							Expression onExpr = join.getOnExpression();

							// shaving of parenthesis
							if (onExpr instanceof Parenthesis) {
								onExpr = ((Parenthesis) onExpr).getExpression();
							}

							if (!(onExpr instanceof EqualsTo)) {
								log.warn("only simple equals statements can be processed, aborting optimization ");
								continue TERMMAPLOOP;
							}

							joinConds.add((EqualsTo) onExpr);

						}
					}
					// create a projection map
					Map<String,Column> projections = new HashMap<String,Column>();
					
					for(SelectItem si : ((PlainSelect) sb).getSelectItems()){
						if(si instanceof SelectExpressionItem){
							if(!(((SelectExpressionItem) si).getExpression() instanceof Column)){
								//no  a column in there, so we skip this query
								continue TERMMAPLOOP;
							}
							Column col = (Column) ((SelectExpressionItem) si).getExpression();
							if(col.getTable().getAlias()==null){
								col.getTable().setAlias(col.getTable().getName());
							}
							String alias = ((SelectExpressionItem) si).getAlias();
							projections.put( alias,col );	
						}
					}
					
					// modify the columns in the term maps
					
					TermMap s = trm.getSubject();
					trm.setSubject(replaceColumn(s, trm, projections, tables, joinConds));
					for(PO po : trm.getPos()){
						po.setObject(replaceColumn(po.getObject(),trm, projections, tables, joinConds));
						po.setPredicate(replaceColumn(po.getPredicate(),trm, projections, tables, joinConds));
					}
					
					log.info("Rewrote query " + trm.getFrom());
					
				}
			}
			
		}
		
	}
	
	private void cleanColumnNames(PlainSelect sb) {
		
		
		SelectVisitor cleaningVisitior = new BaseSelectVisitor(){
			@Override
			public void visit(Column tableColumn) {
				super.visit(tableColumn);

				tableColumn.setColumnName(unescape(tableColumn.getColumnName())); 
				
			}
			
			@Override
			public void visit(Table table) {
				super.visit(table);
				table.setAlias(unescape(table.getAlias()!=null?table.getAlias():table.getName()));
				table.setName(unescape(table.getName()));
				table.setSchemaName(unescape(table.getSchemaName()));
			}
			@Override
			public void visit(SelectExpressionItem selectExpressionItem) {
				super.visit(selectExpressionItem);
				selectExpressionItem.setAlias(unescape(selectExpressionItem.getAlias()));
			}
			
		};
		
		sb.accept(cleaningVisitior);
		

		
	}

	private TermMap replaceColumn(TermMap tm,TripleMap trm, Map<String,Column> name2Col, List<Table> tables, List<EqualsTo> joinConditions){
		List<Expression> expressions =  new ArrayList<Expression>();
		//we use this to make sure constant value triple maps do not get the column set.
		boolean hasReplaced = false;
		for(Expression casted : tm.getExpressions()){
			String castType = DataTypeHelper.getCastType(casted);
			Expression uncast = DataTypeHelper.uncast(casted);
			
			if(uncast instanceof Column){
				Column col =  name2Col.get(((Column) uncast).getColumnName());
				expressions.add(dth.cast(col, castType));
				hasReplaced = true;
				
			}else if (DataTypeHelper.constantValueExpressions.contains(uncast.getClass())){
				expressions.add(dth.cast(uncast, castType));
			}else{
				throw new ImplementationException("unknown expression in TermMap");
			}	
		}
		TermMap newTm =TermMap.createTermMap(dth, expressions);
		if(hasReplaced){
			for(Table table: tables){
				newTm.alias2fromItem.put(table.getAlias(), table);
			}
			newTm.joinConditions.addAll(joinConditions);
		}
		
		newTm.trm = trm;
		

		return newTm; 
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

			// get the referenced map
			Resource parentTripleMap = statement.getObject().asResource();
			for (TripleMap parentTrM : this.tripleMaps.get(parentTripleMap.getURI())) {

				// get the child map
				Resource poMap = reasoningModel.listStatements(null, R2RML.objectMap, objectMap).toList().get(0).getSubject();
				Resource mapping = reasoningModel.listStatements(null, R2RML.predicateObjectMap, poMap).toList().get(0).getSubject();
				for (TripleMap tripleMap : this.tripleMaps.get(mapping.getURI())) {

					// we insert this
					TermMap newoTermMap = parentTrM.getSubject().clone("");

					newoTermMap.trm = parentTrM;

					// get the join condition
					List<Statement> joinconditions = reasoningModel.listStatements(objectMap, R2RML.joinCondition, (RDFNode) null).toList();
					for (Statement joincondition : joinconditions) {
						Resource joinconditionObject = joincondition.getObject().asResource();

						String parentjc = unescape(reasoningModel.listObjectsOfProperty(joinconditionObject, R2RML.parent).toList().get(0).asLiteral()
								.getString());
						String childjc = unescape(reasoningModel.listObjectsOfProperty(joinconditionObject, R2RML.child).toList().get(0).asLiteral()
								.getString());

						this.dbconf.getDataType(parentTrM.from, getRealColumnName(parentjc, parentTrM.from));

						Column leftCol = ColumnHelper.createColumn(parentTrM.from.getAlias(), parentjc);

						Column rightCol = ColumnHelper.createColumn(tripleMap.from.getAlias(), childjc);
						EqualsTo eq = new EqualsTo();
						eq.setLeftExpression(dth.cast(leftCol, dth.getStringCastType()));
						eq.setRightExpression(dth.cast(rightCol, dth.getStringCastType()));
						newoTermMap.getFromJoins().add(eq);
						newoTermMap.addFromItem(tripleMap.from);

						log.debug("Adding join between parent: \"" + parentTrM.from.toString() + "." + parentjc + "\" and child: \"" + tripleMap.from.toString() + "." + childjc + "\"");

					}

					// now we need to create the predicate
					for (RDFNode pnode : reasoningModel.listObjectsOfProperty(poMap, R2RML.predicateMap).toList()) {
						// get the predicate Map
						TermMapQueryResult ptmqr = new TermMapQueryResult(pnode.asResource(), reasoningModel, tripleMap.from);

						TermMap ptm = mapQueryResultOnTermMap(ptmqr, tripleMap.from, tripleMap, R2RML.IRI);

						tripleMap.addPO(ptm, newoTermMap);

					}
				}
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
			
			CompatibilityChecker ccg = new SimpleCompatibilityChecker(tripleMap.getGraph(),this.dbconf,this.dth);
			tripleMap.getGraph().setCompChecker(ccg);

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

	

	

	
	public Set<TripleMap> getTripleMaps() {

		return new HashSet(tripleMaps.values());
	}
	/**
	 * read the prepared model into SparqlMap objects.
	 * 
	 * behold this
	 * 
	 * @throws R2RMLValidationException
	 * @throws JSQLParserException
	 */
	private void loadTripleMaps() throws R2RMLValidationException,
			JSQLParserException {
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
				log.debug("Table: \"" + tablename + "\" is internally referred to as: \"" +tableCount++ + "\"");
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
			Map<Resource,TripleMap> g2trimap  = new HashMap<Resource,TripleMap>();

			// fetch the subject and validate it
			List<Statement> subjectResStmtnt = reasoningModel.listStatements(tmUri, R2RML.subjectMap,(RDFNode) null).toList();
			
			
			// there should only be one
			if (subjectResStmtnt.size() != 1) {
				throw new R2RMLValidationException("Triple map " + tmUri
						+ "has " +subjectResStmtnt.size()+"  subject term map, fix this");
			}
			Resource subjectRes = subjectResStmtnt.get(0).getResource();
			
			TermMapQueryResult sres = new TermMapQueryResult(subjectRes, reasoningModel,
					fromItem);
			
			
			//create the subject term map
			
			TermMap stm = null;

			if (sres.termType != null) {
				stm = mapQueryResultOnTermMap(sres, fromItem,triplemap, sres.termType);
			} else {
				stm = mapQueryResultOnTermMap(sres, fromItem,triplemap, R2RML.IRI);
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
			
			
		
			
	
			List<Statement> postmts = reasoningModel.listStatements(tmUri, R2RML.predicateObjectMap, (RDFNode) null).toList();
			
			Map<TermMap,TripleMap> graph2TripleMap = new HashMap<TermMap,TripleMap>(); 
			

			for  (Statement postmt: postmts) {
				Resource poResource = postmt.getResource();
				
				Resource predicateMapResource = reasoningModel.getProperty(poResource, R2RML.predicateMap).getResource();
				Resource objectMapResource = reasoningModel.getProperty(poResource, R2RML.objectMap).getResource();
				
				
				// get the graph statements for the po here.
				List<Statement> graphMapStmts =  reasoningModel.listStatements(poResource, R2RML.graphMap,(RDFNode) null).toList();
				List<TermMap> graphMaps = getGraphmapsForPO(tmUri, fromItem, graphMapStmts);
				
				
				
				
				
				
				
				
		

				TermMapQueryResult p = new TermMapQueryResult(predicateMapResource,reasoningModel,
						fromItem);
				
				
				if (p.termType != null
						&& !p.termType.getURI().equals(R2RML.IRI)) {
					throw new R2RMLValidationException(
							"Only use iris in predicate position");
				}
		
				TermMap ptm = mapQueryResultOnTermMap(p, fromItem,triplemap,R2RML.IRI);
			


				TermMapQueryResult qr_o = new TermMapQueryResult(objectMapResource,reasoningModel,
						fromItem);
				//the term type definition according to the R2RML spec  http://www.w3.org/TR/r2rml/#termtype
				
				TermMap otm = null;
			
				//Identify the term type here
				if(qr_o.termType != null){
					otm = mapQueryResultOnTermMap(qr_o, fromItem,triplemap,qr_o.termType);
				}else if(qr_o.constant!=null){
					otm = mapQueryResultOnTermMap(qr_o, fromItem, triplemap, null);
				}else if(qr_o.column != null //when column, etc. then it is a literal
							|| qr_o.lang != null
							|| qr_o.datatypeuri != null
							|| (qr_o.termType != null && qr_o.termType.equals(
									R2RML.Literal))){
					otm = mapQueryResultOnTermMap(qr_o, fromItem, triplemap,R2RML.Literal);
					}else{
						//it stays IRI
						
						otm = mapQueryResultOnTermMap(qr_o, fromItem, triplemap,R2RML.IRI);
					}
				
				
				for(TermMap graphTermMap: graphMaps){
					TripleMap graphTripleMap = graph2TripleMap.get(graphTermMap);
					if(graphTripleMap==null){
						graphTripleMap = triplemap.getDeepCopy();
						graphTripleMap.setGraph(graphTermMap);
						graph2TripleMap.put(graphTermMap, graphTripleMap);
					}					
					graphTripleMap.addPO(ptm, otm);	
				}				
				this.tripleMaps.putAll(tmUri.toString(),graph2TripleMap.values());
			}
		}
	}

	public List<TermMap> getGraphmapsForPO(Resource tmUri, FromItem fromItem,
			List<Statement> graphMapStmts) throws R2RMLValidationException {

		List<TermMap> graphMaps = new ArrayList<TermMap>();
		if (graphMapStmts == null || graphMapStmts.isEmpty()) {
			graphMaps = Arrays.asList(this.tfac
					.createTermMap(Quad.defaultGraphIRI));
		} else {
			for (Statement graphMapStmt : graphMapStmts) {
				List<Expression> graph;

				Resource graphMap = graphMapStmt.getResource();
				if (reasoningModel.contains(graphMap, R2RML.template)) {
					String template = reasoningModel.getProperty(graphMap,
							R2RML.template).getString();
					graph = templateToResourceExpression(
							cleanTemplate(template, fromItem), fromItem, dth);
				} else if (reasoningModel.contains(graphMap, R2RML.column)) {
					String column = reasoningModel.getProperty(graphMap,
							R2RML.column).getString();
					String template = "\"{"
							+ getRealColumnName(column, fromItem) + "\"}";
					graph = templateToResourceExpression(
							cleanTemplate(template, fromItem), fromItem, dth);
				} else if (reasoningModel.contains(graphMap, R2RML.constant)) {
					Resource resource = reasoningModel.getProperty(graphMap,
							R2RML.constant).getResource();
					graph = Arrays.asList(resourceToExpression(resource));
				} else {
					throw new R2RMLValidationException(
							"Graphmap without valid value found for "
									+ tmUri.getURI());
				}

				// set the graph
				TermMap gtm = new TermMap(dth);
				gtm.setTermTyp(R2RML.IRI);
				gtm.getResourceColSeg().addAll(graph);

				graphMaps.add(gtm);

			}
		}
		return graphMaps;
	}



	private class TermMapQueryResult {
		
		
		
		public TermMapQueryResult(Resource tm, Model model, FromItem fi){
			template = model.listObjectsOfProperty(tm, R2RML.template).hasNext()?cleanTemplate(model.listObjectsOfProperty(tm, R2RML.template).next().asLiteral().getString(),fi):null;
			
			column = model.listObjectsOfProperty(tm, R2RML.column).hasNext()?getRealColumnName(model.listObjectsOfProperty(tm, R2RML.column).next().asLiteral().getString(), fi):null;
			
			lang = model.listObjectsOfProperty(tm, R2RML.language).hasNext()?model.listObjectsOfProperty(tm, R2RML.language).next().asLiteral().getString():null;
			
			inverseExpression = model.listObjectsOfProperty(tm, R2RML.inverseExpression).hasNext()?model.listObjectsOfProperty(tm, R2RML.inverseExpression).next().asLiteral().getString():null;
			constant = model.listObjectsOfProperty(tm, R2RML.constant).hasNext()?model.listObjectsOfProperty(tm, R2RML.constant).next():null;
			datatypeuri =model.listObjectsOfProperty(tm, R2RML.datatype).hasNext()?model.listObjectsOfProperty(tm, R2RML.datatype).next().asResource():null;		
			termType =  model.listObjectsOfProperty(tm, R2RML.termType).hasNext()?model.listObjectsOfProperty(tm, R2RML.termType).next().asResource():null;
			
			
		}

		
		String[] template;
		String column;
		RDFNode constant;
		String lang;
		Resource datatypeuri;
		String inverseExpression;
		Resource termType;
		
		
	}
	
	public TermMap mapQueryResultOnTermMap(TermMapQueryResult qr, FromItem fi, TripleMap tripleMap, Resource termType){
		
		TermMap tm =  TermMap.createNullTermMap(dth);
		
		if(termType!=null){
			tm.setTermTyp(termType);
		}
		if(qr.constant!=null){
			tm = tfac.createTermMap(qr.constant.asNode());			
		}else if(qr.template!=null){
			if(termType.equals(R2RML.Literal)){
			
				List<Expression> resourceExpression = templateToResourceExpression(qr.template, fi, dth);
				tm.literalValString = FilterUtil.concat(resourceExpression.toArray(new Expression[0]));

			}else{
				List<Expression> resourceExpression = templateToResourceExpression(qr.template, fi, dth);
				tm.getResourceColSeg().addAll(resourceExpression);
			}
			
		}else if(qr.column!=null){
			
			Column col = ColumnHelper.createColumn(fi.getAlias(), qr.column);
			
			if(termType.equals(R2RML.Literal)){
				
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
				//tm.resourceColSeg.add(dth.castNull(dth.getStringCastType()));
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
						ColumnHelper.createColumn(fi.getAlias(), colName),
						dth.getStringCastType()));
			} else {
				newExprs.add(dth.cast(new StringValue("\"" + altSeq.get(i)
						+ "\""), dth.getStringCastType()));
			}
		}
		return newExprs;

	}
	
}
