package org.aksw.sparqlmap.core.config.syntax.r2rml;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.RDF;
/**
 * This class encapsulates the R2RML vocabulary.
 * @author joerg
 *
 */
public abstract class R2RML {
  
 

  public static final  String R2RML_STRING = "http://www.w3.org/ns/r2rml#";

  public static final String BaseTableOrView_STRING = R2RML_STRING + "BaseTableOrView";

  public static final String BlankNode_STRING = R2RML_STRING + "BlankNode";

  public static final String GraphMap_STRING = R2RML_STRING + "GraphMap";

  public static final String IRI_STRING = R2RML_STRING + "IRI";

  public static final String Join_STRING = R2RML_STRING + "Join";

  public static final String Literal_STRING = R2RML_STRING + "Literal";

  public static final String LogicalTable_STRING = R2RML_STRING + "LogicalTable";

  public static final String ObjectMap_STRING = R2RML_STRING + "ObjectMap";

  public static final String PredicateMap_STRING = R2RML_STRING + "PredicateMap";

  public static final String PredicateObjectMap_STRING = R2RML_STRING + "PredicateObjectMap";

  public static final String RefObjectMap_STRING = R2RML_STRING + "RefObjectMap";

  public static final String R2RMLView_STRING = R2RML_STRING + "R2RMLView";

  public static final String SubjectMap_STRING = R2RML_STRING + "SubjectMap";

  public static final String TriplesMap_STRING = R2RML_STRING + "TriplesMap";

  public static final String child_STRING = R2RML_STRING + "child";

  public static final String hasClass_STRING = R2RML_STRING + "class";

  public static final String column_STRING = R2RML_STRING + "column";

  public static final String constant_STRING = R2RML_STRING + "constant";

  public static final String datatype_STRING = R2RML_STRING + "datatype";

  public static final String graph_STRING = R2RML_STRING + "graph";

  public static final String graphMap_STRING = R2RML_STRING + "graphMap";

  public static final String inverseExpression_STRING = R2RML_STRING + "inverseExpression";

  public static final String joinCondition_STRING = R2RML_STRING + "joinCondition";

  public static final String language_STRING = R2RML_STRING + "language";

  public static final String logicalTable_STRING = R2RML_STRING + "logicalTable";

  public static final String object_STRING = R2RML_STRING + "object";

  public static final String objectMap_STRING = R2RML_STRING + "objectMap";

  public static final String parent_STRING = R2RML_STRING + "parent";

  public static final String parentTriplesMap_STRING = R2RML_STRING + "parentTriplesMap";

  public static final String predicate_STRING = R2RML_STRING + "predicate";

  public static final String predicateMap_STRING = R2RML_STRING + "predicateMap";

  public static final String predicateObjectMap_STRING = R2RML_STRING + "predicateObjectMap";

  public static final String refObjectMap_STRING = R2RML_STRING + "refObjectMap";

  public static final String sqlQuery_STRING = R2RML_STRING + "sqlQuery";

  public static final String sqlVersion_STRING = R2RML_STRING + "sqlVersion";

  public static final String subject_STRING = R2RML_STRING + "subject";

  public static final String subjectMap_STRING = R2RML_STRING + "subjectMap";

  public static final String tableName_STRING = R2RML_STRING + "tableName";

  public static final String template_STRING = R2RML_STRING + "template";

  public static final String termType_STRING = R2RML_STRING + "termType";

  public static final String SQL2008_STRING = R2RML_STRING + "SQL2008";

  public static final String defaultGraph_STRING = R2RML_STRING + "defaultGraph";

  public static final Resource BaseTableOrView = createResource(BaseTableOrView_STRING);

  public static final Resource BlankNode = createResource(BlankNode_STRING);

  public static final Resource GraphMap = createResource(GraphMap_STRING);

  public static final Resource IRI = createResource(IRI_STRING);

  public static final Resource Join = createResource(Join_STRING);

  public static final Resource Literal = createResource(Literal_STRING);

  public static final Resource LogicalTable = createResource(LogicalTable_STRING);

  public static final Resource ObjectMap = createResource(ObjectMap_STRING);

  public static final Resource PredicateMap = createResource(PredicateMap_STRING);

  public static final Resource PredicateObjectMap = createResource(PredicateObjectMap_STRING);

  public static final Resource RefObjectMap = createResource(RefObjectMap_STRING);

  public static final Resource R2RMLView = createResource(R2RMLView_STRING);

  public static final Resource SubjectMap = createResource(SubjectMap_STRING);

  public static final Resource TriplesMap = createResource(TriplesMap_STRING);

  public static final Property child = createProprty(child_STRING);

  public static final Property hasClass = createProprty(hasClass_STRING);

  public static final Property column = createProprty(column_STRING);

  public static final Property constant = createProprty(constant_STRING);

  public static final Property datatype = createProprty(datatype_STRING);

  public static final Property graph = createProprty(graph_STRING);

  public static final Property graphMap = createProprty(graphMap_STRING);

  public static final Property inverseExpression = createProprty(inverseExpression_STRING);

  public static final Property joinCondition = createProprty(joinCondition_STRING);

  public static final Property language = createProprty(language_STRING);

  public static final Property logicalTable = createProprty(logicalTable_STRING);

  public static final Property object = createProprty(object_STRING);

  public static final Property objectMap = createProprty(objectMap_STRING);

  public static final Property parent = createProprty(parent_STRING);

  public static final Property parentTriplesMap = createProprty(parentTriplesMap_STRING);

  public static final Property predicate = createProprty(predicate_STRING);

  public static final Property predicateMap = createProprty(predicateMap_STRING);

  public static final Property predicateObjectMap = createProprty(predicateObjectMap_STRING);

  public static final Property refObjectMap = createProprty(refObjectMap_STRING);

  public static final Property sqlQuery = createProprty(sqlQuery_STRING);

  public static final Property sqlVersion = createProprty(sqlVersion_STRING);

  public static final Property subject = createProprty(subject_STRING);

  public static final Property subjectMap = createProprty(subjectMap_STRING);

  public static final Property tableName = createProprty(tableName_STRING);

  public static final Property template = createProprty(template_STRING);

  public static final Property termType = createProprty(termType_STRING);

  public static final Resource SQL2008 = createResource(SQL2008_STRING);

  public static final Resource defaultGraph = createResource(defaultGraph_STRING);

  private static Resource createResource(String uri) {
    return ResourceFactory.createResource(uri);
  }

  private static Property createProprty(String uri) {
    return ResourceFactory.createProperty(uri);
  }
  

}
