package org.aksw.sparqlmap.config.syntax;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.aksw.sparqlmap.mapper.subquerymapper.algebra.FilterUtil;
import org.apache.commons.collections15.MultiMap;
import org.apache.commons.collections15.multimap.MultiHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;

/**
 * This class manages a set of mappings.
 * 
 * @author joerg
 * 
 */
public class MappingConfiguration {

	private R2RConfiguration r2rconf;

	public MappingConfiguration(R2RConfiguration r2rconf) {
		super();
		this.r2rconf = r2rconf;
	}

	public R2RConfiguration getR2rconf() {
		return r2rconf;
	}

	private static Logger log = LoggerFactory
			.getLogger(MappingConfiguration.class);

	private List<Mapping> mappings = new ArrayList<Mapping>();

	private Map<String, Mapping> mappingAliases = new HashMap<String, Mapping>();

	private MultiMap<String, Mapping> linkedDataPath2mapping = new MultiHashMap<String, Mapping>();

	// URL of a rdfs:Property to Mapping
	private MultiMap<String, ColumDefinition> prop2Column = new MultiHashMap<String, ColumDefinition>();

	private MultiMap<String, Mapping> prop2Mapping = new MultiHashMap<String, Mapping>();

	// URL of a rdfs:Class to Mapping
	private MultiMap<Resource, Mapping> type2Mapping = new MultiHashMap<Resource, Mapping>();
	
	
	private FilterUtil filterUtil = new FilterUtil(this);
	
	public FilterUtil getFilterUtil() {
		return filterUtil;
	}
	
	public List<Mapping> getMappings() {
		return mappings;

	}
	

	/**
	 * all mappings related to this property, in particulat all mappings that
	 * have a common linked data path and at least of this group has this
	 * property.
	 * 
	 * @param prop
	 * @return
	 */
	public Collection<Mapping> getMappingForProperty(String prop) {
		Set<Mapping> mappings = new HashSet<Mapping>();
		Collection<Mapping> directlyReferenced = prop2Mapping.get(prop);
		if (directlyReferenced != null) {
			for (Mapping mapping : directlyReferenced) {
				mappings.addAll(linkedDataPath2mapping.get(mapping
						.getIdColumn().getLinkedDataPath()));
			}
		}
		return mappings;
	}
	
	public Collection<String> getPathForProperty(String prop){
		return getPathsForMappings(getMappingForProperty(prop));
	}
	
	
	
	public List<ColumDefinition> getColumnForProperty(String prop) {
		return (List<ColumDefinition>) prop2Column.get(prop);
	}

	/**
	 * returns all mappings that can be associated with this rdf:type. this
	 * includes not only the ones explicitly stated, but also the ones included
	 * in other mappings with the same linked data path.
	 * 
	 * @param type
	 * @return
	 */
	public Collection<Mapping> getMappingForType(Resource type) {
		Collection<Mapping> mappings = new HashSet<Mapping>();
		// get Mappings first.
		Collection<Mapping> directlyTypedMappings = type2Mapping.get(type);
		if (directlyTypedMappings != null) {

			for (Mapping mapping : directlyTypedMappings) {
				// first add it self
				mappings.add(mapping);
				// then all others with the same linked data path
				mappings.addAll(linkedDataPath2mapping.get(mapping.getIdColumn().getLinkedDataPath()));
			}

			// then iterate over them to retrieve the list of other mappings for
			// this path

			if (type2Mapping.containsKey(type)) {
				mappings.addAll((List<Mapping>) type2Mapping.get(type));
			}
		}
		
		// we also have to add all mappings with the rdf:type defined as in a relation sort of way
		
		mappings.addAll(getMappingForProperty(RDF.type.getURI()));

		return mappings;
	}
	
	public static Collection<String> getPathsForMappings(Collection<Mapping> mappings){
		Set<String> linkedDataPaths = new HashSet<String>();
		for (Mapping mapping : mappings) {
			linkedDataPaths.add(mapping.getIdColumn().getLinkedDataPath());
		}
		return linkedDataPaths;
	}
	
	
	public void fillMaps(){
		for(Mapping mapping: mappings){
			linkedDataPath2mapping.put(mapping.getIdColumn().getLinkedDataPath(), mapping);
		}
	}

	
	

	public void add(Mapping mapping) {

		// assign to the From item a unique alias. this is just a number right
		// now.

		//mapping.getFromPart().setAlias("alias_" + fromItemCount++);
		mappingAliases.put(mapping.getFromPart().getAlias(), mapping);
		
		mappings.add(mapping);

		type2Mapping.put(mapping.getTypeOf(), mapping);

		// register everything in the appropriate maps.

		for (ColumDefinition colDef : mapping.getColDefinitions()) {
			prop2Column.put(colDef.getProperty(), colDef);
			prop2Mapping.put(colDef.getProperty(), mapping);

		}

	}

	/**
	 * Returns all mappings that are able to produce this url as their subject.
	 * 
	 * @param uri
	 * @return
	 */
	public Set<Mapping> getMappingForInstanceUri(String uri) {
		
		Set<Mapping> mappings = new HashSet<Mapping>();
		for(Mapping mapping: this.mappingAliases.values()){
			if(mapping.getIdColumn().getLdPattern().matcher(uri).matches()){
				mappings.add(mapping);
			}
		}
		return mappings;
	}
	
	
	public Collection<String> getPathForInstanceUri(String uri){
		return getPathsForMappings(getMappingForInstanceUri(uri));	
	}
	


	/**
	 * returns only the id part of a uri, removing the linked data prefix
	 * 
	 * @param uri
	 * @return
	 */
	public String getIdForInstanceUri(String uri) {

		String id = null;
		// consider only the part of the instance uri, denoting the different
		// instance types.
		List<String> linkedDataPaths = new ArrayList<String>();
		linkedDataPaths.addAll(linkedDataPath2mapping.keySet());
		Collections.sort(linkedDataPaths, new Comparator<String>() {
			public int compare(String o1, String o2) {
				if (o1.length() < o2.length()) {
					return 1;
				} else if (o1.length() > o2.length()) {
					return -1;
				} else {
					return 0;
				}
			}
		});

		// we check if for the first (best matching) uri
		for (String linkedDataPath : linkedDataPaths) {
			if (uri.startsWith(linkedDataPath)) {
				id = uri.substring(linkedDataPath.length());
				break;
			}

		}

		return id;
	}


	public Set<String> getLinkedDataPaths() {
		Set<String> copy = new HashSet<String>();
		copy.addAll(linkedDataPath2mapping.keySet());
		return copy;
	}
	
	
	/**
	 * returns all mappings with a certain linked data path
	 * @param linkedDataPath
	 * @return
	 */
	public Collection<Mapping> getMappings(String linkedDataPath){		
		
		return new HashSet<Mapping>(linkedDataPath2mapping.get(linkedDataPath));

	}
	
	/**
	 * the mapping that maps an aliased sql construct.
	 * @param alias
	 * @return
	 */
	public Mapping getMappingForAlias(String alias){
		return this.mappingAliases.get(alias);
	}
	
	/**
	 * return the column identified by the alias of the sql construct and the col name
	 * 
	 * @return
	 */
	public ColumDefinition getColumnForName(String fromAlias, String colname){
		
		return getMappingForAlias(fromAlias).getColumnDefinition(colname);
		
		
	}
	
	
	public Collection<Mapping> getJoinsWith(Mapping mapping){
		List<Mapping> joins = new ArrayList<Mapping>();
		
		for(ColumDefinition coldef: mapping.getColDefinitions()){
			joins.addAll(getJoinsWith(coldef));
		}
		
		return joins;
	}
	
	
	public Collection<Mapping> getJoinsWith(ColumDefinition coldef){
		if(coldef.getJoinsAlias()!=null){

			return getMappings(getMappingForAlias(coldef.getJoinsAlias()).getIdColumn().getLinkedDataPath());
		}else{
			return new ArrayList<Mapping>();
		}

		
	}
	

}
