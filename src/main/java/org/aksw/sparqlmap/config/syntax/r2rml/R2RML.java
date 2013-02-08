package org.aksw.sparqlmap.config.syntax.r2rml;


import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class R2RML {

	public static String R2RML_STRING = "http://www.w3.org/ns/r2rml#";
	public static String BaseTableOrView_STRING = R2RML_STRING + "BaseTableOrView";
	public static String BlankNode_STRING = R2RML_STRING + "BlankNode";
	public static String GraphMap_STRING = R2RML_STRING + "GraphMap";
	public static String IRI_STRING = R2RML_STRING + "IRI";
	public static String Join_STRING = R2RML_STRING + "Join";
	public static String Literal_STRING = R2RML_STRING + "Literal";
	public static String LogicalTable_STRING = R2RML_STRING + "LogicalTable";
	public static String ObjectMap_STRING = R2RML_STRING + "ObjectMap";
	public static String PredicateMap_STRING = R2RML_STRING + "PredicateMap";
	public static String PredicateObjectMap_STRING = R2RML_STRING + "PredicateObjectMap";
	public static String RefObjectMap_STRING = R2RML_STRING + "RefObjectMap";
	public static String R2RMLView_STRING = R2RML_STRING + "R2RMLView";
	public static String SubjectMap_STRING = R2RML_STRING + "SubjectMap";
	public static String TriplesMap_STRING = R2RML_STRING + "TriplesMap";

	public static String child_STRING = R2RML_STRING + "child";
	public static String hasClass_STRING = R2RML_STRING + "class";
	public static String column_STRING = R2RML_STRING + "column";
	public static String constant_STRING = R2RML_STRING + "constant";
	public static String datatype_STRING = R2RML_STRING + "datatype";
	public static String graph_STRING = R2RML_STRING + "graph";
	public static String graphMap_STRING = R2RML_STRING + "graphMap";
	public static String inverseExpression_STRING = R2RML_STRING + "inverseExpression";
	public static String joinCondition_STRING = R2RML_STRING + "joinCondition";
	public static String language_STRING = R2RML_STRING + "language";
	public static String logicalTable_STRING = R2RML_STRING + "logicalTable";
	public static String object_STRING = R2RML_STRING + "object";
	public static String objectMap_STRING = R2RML_STRING + "objectMap";
	public static String parent_STRING = R2RML_STRING + "parent";
	public static String parentTriplesMap_STRING = R2RML_STRING + "parentTriplesMap";
	public static String predicate_STRING = R2RML_STRING + "predicate";
	public static String predicateMap_STRING = R2RML_STRING + "predicateMap";
	public static String predicateObjectMap_STRING = R2RML_STRING + "predicateObjectMap";
	public static String refObjectMap_STRING = R2RML_STRING + "refObjectMap";
	public static String sqlQuery_STRING = R2RML_STRING + "sqlQuery";
	public static String sqlVersion_STRING = R2RML_STRING + "sqlVersion";
	public static String subject_STRING = R2RML_STRING + "subject";
	public static String subjectMap_STRING = R2RML_STRING + "subjectMap";
	public static String tableName_STRING = R2RML_STRING + "tableName";
	public static String template_STRING = R2RML_STRING + "template";
	public static String termType_STRING = R2RML_STRING + "termType";
	
	
	public static String SQL2008_STRING = R2RML_STRING + "SQL2008";
	public static String defaultGraph_STRING = R2RML_STRING + "defaultGraph";
	
	

	public static Resource BaseTableOrView = createResource(BaseTableOrView_STRING);
	public static Resource BlankNode = createResource(BlankNode_STRING);
	public static Resource GraphMap = createResource(GraphMap_STRING);
	public static Resource IRI =createResource(IRI_STRING);
	public static Resource Join = createResource(Join_STRING);
	public static Resource Literal = createResource(Literal_STRING);
	public static Resource LogicalTable = createResource(LogicalTable_STRING);
	public static Resource ObjectMap = createResource(ObjectMap_STRING);
	public static Resource PredicateMap = createResource(PredicateMap_STRING);
	public static Resource PredicateObjectMap = createResource(PredicateObjectMap_STRING);
	public static Resource RefObjectMap = createResource(RefObjectMap_STRING);
	public static Resource R2RMLView = createResource(R2RMLView_STRING);
	public static Resource SubjectMap = createResource(SubjectMap_STRING);
	public static Resource TriplesMap = createResource(TriplesMap_STRING);

	public static Property child = createProprty(child_STRING);
	public static Property hasClass = createProprty(hasClass_STRING);
	public static Property column = createProprty(column_STRING);
	public static Property constant = createProprty(constant_STRING);
	public static Property datatype = createProprty(datatype_STRING);
	public static Property graph = createProprty(graph_STRING);
	public static Property graphMap = createProprty(graphMap_STRING);
	public static Property inverseExpression = createProprty(inverseExpression_STRING);
	public static Property joinCondition = createProprty(joinCondition_STRING);
	public static Property language = createProprty(language_STRING);
	public static Property logicalTable = createProprty(logicalTable_STRING);
	public static Property object = createProprty(object_STRING);
	public static Property objectMap = createProprty(objectMap_STRING);
	public static Property parent = createProprty(parent_STRING);
	public static Property parentTriplesMap = createProprty(parentTriplesMap_STRING);
	public static Property predicate = createProprty(predicate_STRING);
	public static Property predicateMap = createProprty(predicateMap_STRING);
	public static Property predicateObjectMap = createProprty(predicateObjectMap_STRING);
	public static Property refObjectMap = createProprty(refObjectMap_STRING);
	public static Property sqlQuery = createProprty(sqlQuery_STRING);
	public static Property sqlVersion = createProprty(sqlVersion_STRING);
	public static Property subject = createProprty(subject_STRING);
	public static Property subjectMap = createProprty(subjectMap_STRING);
	public static Property tableName = createProprty(tableName_STRING);
	public static Property template = createProprty(template_STRING);
	public static Property termType = createProprty(termType_STRING);
	
	
	public static Resource SQL2008 = createResource(SQL2008_STRING);
	public static Resource defaultGraph = createResource(defaultGraph_STRING);
	
	private static Resource createResource(String uri){
		return ResourceFactory.createResource(uri);
	}
	
	private static Property createProprty(String uri){
		return ResourceFactory.createProperty(uri);
	}
	
}
