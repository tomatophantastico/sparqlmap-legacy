package org.aksw.sparqlmap.core.db;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.aksw.sparqlmap.core.SystemInitializationError;
import org.apache.jena.atlas.logging.Log;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;

public class CSVHelper {

  private static org.slf4j.Logger log = LoggerFactory
    .getLogger(CSVHelper.class);

  public static List<CSVColumnConfig> getColumns(CSVTableConfig csvconf)
    throws IOException {

    CsvPreference prefs =
      new CsvPreference.Builder("\"".charAt(0), csvconf.fs_interpreted, "\n")
        .build();

    log.info(String.format(
      "Start reading file: %s for determing column and row count.",
      csvconf.file.getName()));

    CsvListReader reader =
      new CsvListReader(new FileReader(csvconf.file), prefs);

    int rowCount = 0;
    int colCountMax = -1;
    while (true) {
      List<String> row = reader.read();

      if (row != null) {
        rowCount++;
      } else {
        break;
      }

      if (colCountMax == -1) {
        colCountMax = row.size();
      }
      if (row.size() != colCountMax && csvconf.consistentcolcount) {
        String message =
          String
            .format(
              "Inconsistent col count detected. row: %d has has %d cols. As separator '%s' was used. If the file has varying col count use the appropriate option.",
              rowCount, colCountMax, csvconf.fs);

        throw new SystemInitializationError(message);
      }

      if (colCountMax < row.size()) {
        colCountMax = row.size();
      }
    }
    reader.close();
    log.info(String.format("File: %s has %d rows and %d columns",
      csvconf.file.getName(), rowCount, colCountMax));

    log.info(String.format(
      "Finished reading file: %s for determing column and row count.",
      csvconf.file.getName()));

    log.info(String.format("Start reading file: %s for column type checking",
      csvconf.file.getName()));

    reader = new CsvListReader(new FileReader(csvconf.file), prefs);

    List<String> headers = null;

    if (csvconf.hasHeaderRow) {
      // we use the header here
      headers = new ArrayList<String>();

      for (String header : reader.getHeader(true)) {
        headers.add(header.replaceAll("[^a-zA-Z0-9\\s]", "_"));
      }

      if (new HashSet<String>(headers).size() != headers.size()) {
        log
          .warn("Headers of the csv appear not to be unique after removing non-alphanumeric characters");
        List<String> newheaders = new ArrayList<String>();
        for (int i = 0; i < headers.size(); i++) {
          newheaders.add(headers.get(i) + "_" + i);
        }
        headers = newheaders;
      }

      if (headers.size() < colCountMax) {
        log.warn("Thereare more columns than headers. Adding headers.");

        for (int i = 0; i < (colCountMax - headers.size()); i++) {
          headers.add("col_" + (i + headers.size()));
        }
      }

    } else {
      headers = new ArrayList<String>();
      for (int c = 1; c <= colCountMax; c++) {
        headers.add("col_" + c);
      }
    }

    List<CellProcessor> cellprocessors = new ArrayList<CellProcessor>();
    for (int i = 0; i < colCountMax; i++) {
      CellProcessor cp = new DataTypeFindingCellProcessor(headers.get(i));
      cellprocessors.add(cp);
    }
    CellProcessor[] processors = cellprocessors.toArray(new CellProcessor[0]);

    // now go through the sheet

    while (true) {
      List<Object> rowobjects = reader.read(processors);
      if (rowobjects == null) {
        break;
      }
    }
    reader.close();

    List<CSVColumnConfig> colconfs = new ArrayList<CSVColumnConfig>();
    for (CellProcessor proc : processors) {

      DataTypeFindingCellProcessor dfp = (DataTypeFindingCellProcessor) proc;
      CSVColumnConfig colConf = dfp.getCSVColumnConfig();
      colconfs.add(colConf);
    }

    log.info(String.format(
      "Finished reading file: %s for column type checking",
      csvconf.file.getName()));

    log.info("The following columns were determined:");

    for (CSVColumnConfig ccc : colconfs) {
      log.info(String.format("Column: %-20s %s ", ccc.name, ccc.datatype));
    }

    return colconfs;

  }

  public static class DataTypeFindingCellProcessor extends CellProcessorAdaptor {

    String name;

    boolean hasNull = false;

    int size = -1;

    List<CellProcessor> processors = new ArrayList<CellProcessor>(
      Arrays.asList((CellProcessor) new ParseDouble(),
        (CellProcessor) new ParseInt()));

    public DataTypeFindingCellProcessor(String name) {
      this.name = name;
    }

    @Override
    public Object execute(Object value, CsvContext context) {

      if (value == null) {
        hasNull = true;
      } else {
        if (processors.size() > 0) {
          List<CellProcessor> toremove = new ArrayList<CellProcessor>();
          for (CellProcessor proc : processors) {
            try {
              proc.execute(value, context);
            } catch (SuperCsvCellProcessorException e) {
              toremove.add(proc);
            }
          }
          processors.removeAll(toremove);

        }
        if (processors.size() == 0) {
          String valString = (String) value;
          if (valString.length() > size) {
            size = valString.length();
          }
        }

      }
      return value;
    }

    public CSVColumnConfig getCSVColumnConfig() {
      CSVColumnConfig csvc = new CSVColumnConfig();
      if (processors.isEmpty()) {
        csvc.datatype = String.format("VARCHAR (%s)", size);
      } else {
        CellProcessor firstCell = processors.get(0);
        if (firstCell instanceof ParseDouble) {
          csvc.datatype = "DECIMAL";
        } else if (firstCell instanceof ParseInt) {
          csvc.datatype = "BIGINT";
        }
      }

      csvc.name = name;
      csvc.isNullable = hasNull;

      return csvc;
    }
  }

  public static class CSVColumnConfig {

    public String name;

    public String datatype;

    public boolean isNullable;

  }

  public static class CSVTableConfig {
    public boolean consistentcolcount;

    public String fs = ",";

    public Character fs_interpreted;

    public String name;

    public File file;

    public List<String> columnlabels = new ArrayList<String>();

    public Boolean hasHeaderRow = false;

    public Boolean quoted = true;

    public Boolean allQuoted = false;

    public Integer cacheRows = 1000;

    public Integer cacheSize = 10000;

    public String encoding;

  }

}
