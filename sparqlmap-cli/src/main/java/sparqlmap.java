import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Properties;

import org.aksw.sparqlmap.core.SparqlMap;
import org.aksw.sparqlmap.core.automapper.AutomapperWrapper;
import org.aksw.sparqlmap.core.config.syntax.r2rml.R2RMLValidationException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFFormatVariant;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.WebContent;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.PropertiesPropertySource;

import com.hp.hpl.jena.rdf.model.Model;

public class sparqlmap {

	static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(sparqlmap.class);

	public sparqlmap() {
		this.out = System.out;
		this.err = System.err;
	}
	
	public sparqlmap(PrintStream out, PrintStream err) {
		super();
		this.out = out;
		this.err = err;
	}
	

	PrintStream out;
	PrintStream err;



	
	private Options options;
	private AnnotationConfigApplicationContext ctxt;

	public AnnotationConfigApplicationContext setupSparqlMap(
			Properties... props) throws Throwable {

		AnnotationConfigApplicationContext ctxt;

		ctxt = new AnnotationConfigApplicationContext();

		for (int i = 0; i < props.length; i++) {
			ctxt.getEnvironment()
					.getPropertySources()
					.addFirst(new PropertiesPropertySource("props " + i, props[i]));
		}

		ctxt.scan("org.aksw.sparqlmap");
		ctxt.refresh();
		return ctxt;

	}

	private Throwable getExceptionCause(BeansException be) {
		if (be.getCause() instanceof BeansException) {
			return getExceptionCause((BeansException) be.getCause());
		} else {
			return be.getCause();
		}
	}

	public static Options getOptions() {
		
		Options options = new Options();
		
		OptionGroup action = new OptionGroup();
		action.addOption(OptionBuilder.withDescription("Writes an rdf dump into stdout according to the supplied mapping file").create("dump"));
		action.addOption(OptionBuilder.withDescription("Creates a mapping file that maps the specified database into R2RML according to the direct mapping specification").create("generateMapping"));
		action.addOption(OptionBuilder.withDescription("Reads an CSV-file and dumps the contents as rdf according to the mapping filde.").create("dump-csv"));
		action.addOption(OptionBuilder.withDescription("Creates a mapping file according for a CSV file.").create("generateMapping-csv"));
		
		options.addOptionGroup(action);
		
		options.addOption(OptionBuilder
				.withArgName("db-file")
				.hasArg()
				.withDescription(
						"A file properties file containing the parameters for connecting to the database.")
				.create("dbfile"));
		options.addOption(OptionBuilder
				.withArgName("jdbc-url")
				.hasArg()
				.withDescription(
						"the full connection url for the database, like it is used for connection in java to a db. Example: jdbc:postgresql://localhost/mydb")
				.create("dburi"));
		options.addOption(OptionBuilder.withArgName("db-username").hasArg()
				.withDescription("username for the db connection")
				.create("dbuser"));
		options.addOption(OptionBuilder.withArgName("db-password").hasArg()
				.withDescription("password for the db connection")
				.create("dbpass"));
		options.addOption(OptionBuilder
				.withArgName("base-iri")
				.hasArg()
				.withDescription(
						"Base iri used for cases in the mapping process, where no explicit iri is defined.")
				.create("baseiri"));
		options.addOption(OptionBuilder.withArgName("r2rmlfile").hasArg().withDescription("The R2RML file according which defines the mapping for the dump.").create("r2rmlfile"));		
		options.addOption(OptionBuilder.withArgName("sparqlmapfile").hasArg().withDescription("A properties file that configures SparqlMap. Usually contains the properties given here as options. Explicit options override the values of the properties file.").create("sparqlmapfile"));
		options.addOption(OptionBuilder.withArgName("file").withDescription("CSV File name").create("csv-file"));
		options.addOption(OptionBuilder.withDescription("The first line of the file describes the headers").create("csv-hasHeaders"));
		options.addOption(OptionBuilder.withArgName("char").withDescription("The column separator character").create("csv-sepchar"));
		
		options.addOption(OptionBuilder.withArgName("outputformat").hasArg().withDescription("The output format name. Values are: RDF/XML, Turtle, N-TRiples, N3, RDF/JSON, N-Quads, TriG. Defaults to N-Triples.").create("outputformat"));
		return options;
	}

	public void processCommand(String[] args) {
		try {
			CommandLineParser clparser = new PosixParser();
			// get the default
			options = getOptions();
			// get the command specific
			// process the commandline args
			CommandLine cl = null;

			cl = clparser.parse(options, args);

			// process the basic options
			Properties props = new Properties();
			if (cl.hasOption("baseiri")) {
				props.setProperty("sm.baseuri", cl.getOptionValue("baseiri"));
			} else {
				props.setProperty("sm.baseuri", "http://localhost/baseuri/");
			}

			// process the database options
			processDbOptions(options, cl, props);
			String outputlang = null;
			if(cl.hasOption("outputformat")){
				outputlang =  cl.getOptionValue("outputformat");
			}
			
			
			// set the db mapping
			if(cl.hasOption("r2rmlfile")){
				props.setProperty("sm.mappingfile", cl.getOptionValue("r2rmlfile"));
			}
			

			//setupt the context
			
			
			// perform the specified action
			
			if(cl.hasOption("dump")){
				//validate conf
				if(cl.hasOption("r2rmlfile")){
					System.err.println("Creating an RDF dump.");
					this.ctxt = setupSparqlMap(props);
					dump(outputlang);
				}else{
					error("For -dump, please provide an R2RML file.");
				}
				
				
			}else if(cl.hasOption("generateMapping")){
				
				
				//check if there is no mapping file given
				if(!cl.hasOption("r2rmlfile")){
					System.err.println("Creating R2RML based on Direct Mapping");
					this.ctxt = setupSparqlMap(props);

					generateMapping(outputlang);
				}else{
					error("for generateMapping do not use the -r2rmlfile option.");
				}
				
				
				
			} 

			
		} catch (ParseException e) {
			error(e.getMessage());
		} catch (FileNotFoundException e) {
			error("File not found: " + e.getMessage());
		} catch (IOException e) {
			error("Error reading file: " + e.getMessage());
		} catch (R2RMLValidationException e) {
			error("Error validation the mapping file: " + e.getMessage());
		} catch (BeansException e) {
			err.println("Error during setup: "
					+ getExceptionCause(e).getMessage());

		} catch (Throwable t) {
			log.error("Another error happened: ", t);
			err.println("Error setting up the app: " + t.getMessage());

		}

	}

	private void processDbOptions(Options options, CommandLine cl,
			Properties props) throws MissingOptionException,
			FileNotFoundException, IOException {
		if (cl.hasOption("dbfile")) {
			// load db file

			props.load(new FileReader(cl.getOptionValue("dbfile")));

		} else if (cl.hasOption("dbuser") && cl.hasOption("dbpass")
				&& cl.hasOption("dburi")) {
			//

			props.setProperty("jdbc.url", cl.getOptionValue("dburi"));
			props.setProperty("jdbc.username", cl.getOptionValue("dbuser"));
			props.setProperty("jdbc.password", cl.getOptionValue("dbpass"));
		} else {
			throw new MissingOptionException(
					"Supply the db connection information by either supplying a file or the db options");
		}

		props.setProperty("jdbc.poolminconnections", "5");
		props.setProperty("jdbc.poolmaxconnections", "10");
	}

	public void error(String message) {
		HelpFormatter formatter = new HelpFormatter();
		err.println(message);
		PrintWriter pw = new PrintWriter(out);
		formatter.printHelp("java -jar <pathtojar> ", options);
		pw.flush();
	}


	
	
	
	
	public void dump(String langoutputf) throws SQLException{
		if(langoutputf==null){
			langoutputf = WebContent.contentTypeNTriples;
		}
		
		SparqlMap sm = ctxt.getBean(SparqlMap.class);
		
		sm.dump(System.out, langoutputf);
		
	}
	

	
	
	public void generateMapping(String format) throws FileNotFoundException, SQLException{
		
		AutomapperWrapper am = ctxt.getBean(AutomapperWrapper.class);
		
		if(format==null){
			format = WebContent.contentTypeTurtle;
		}
		
		if (!format.equals(Lang.TURTLE)){
			log.warn("For generating a mapping TURTLE is the recommended output format. ");
		}
		
				
		RDFDataMgr.write(out, am.automap(), RDFLanguages.nameToLang(format));
		
	}

	
	
	public static void main(String[] args) {
		
		sparqlmap smcl = new sparqlmap();
		smcl.processCommand(args);
		
	}
}
