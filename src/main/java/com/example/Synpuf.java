package com.example;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.opencsv.CSVParser;
import java.io.IOException;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Count;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import java.util.HashMap;
import com.utils.*;
import java.util.ArrayList;
public class Synpuf
{
 private static final byte[] FAMILY = Bytes.toBytes("beneficiary-summary");
  private static final byte[] beneficiry_id = Bytes.toBytes("beneficiry_id");
    private static final byte[] death_date = Bytes.toBytes("death_date");
   private static long row_id = 0;
    //private static final byte[] SEX = Bytes.toBytes("sex");

static final DoFn<String, TableRow> MUTATION_TRANSFORM = new DoFn<String, TableRow>() {
  private static final long serialVersionUID = 1L;

  @Override
  public void processElement(DoFn<String, TableRow>.ProcessContext c) throws Exception {
  	String line = c.element();
   	JSONArray indicationObject =null;
	String patientId = null, startDate = null,startMonth,startYear,startDate,kind=null,e_id = null ;
	JSONParser parser = new JSONParser();
	try {
		Object obj = parser.parse(line);
		JSONObject jsonObject = (JSONObject) obj;
		JSONArray resource = (JSONArray) jsonObject.get("resources");
		for (int i = 0; i < resource.size(); i++) {
			put_object = new Put(Bytes.toBytes(row_id));
   			row_id = row_id +1;
			JSONObject jsonObject1 = (JSONObject) parser.parse(resource.get(i).toString());
			HashMap map  = (HashMap) jsonObject1.get("resource");
			HashMap<String , JSONArray> map2  =  (HashMap<String, JSONArray>) jsonObject1.get("resource");
			JSONObject patientObj  =  (JSONObject) map.get("patient");
			String patient =  (String) patientObj.get("reference");
			patientId = patient.substring(patient.indexOf('/')+1);
			JSONObject periodObj  =  (JSONObject) map.get("period");
			startDate =  (String) periodObj.get("start");
			kind =  (String) map.get("class");
		        e_id = (String) map.get("id"); 
			TableRow row = new TableRow().set("month", c.element().getKey()).set("tornado_count", c.element().getValue());
     			c.output(row);
			put_object.addColumn(FAMILY, E_ID, Bytes.toBytes(e_id));
			put_object.addColumn(FAMILY, STARTDATE, Bytes.toBytes(startDate));
			put_object.addColumn(FAMILY, ENDDATE, Bytes.toBytes(endDate));
			put_object.addColumn(FAMILY, STARTTIME, Bytes.toBytes(startTime));
			put_object.addColumn(FAMILY, P_ID, Bytes.toBytes(patientId));
			put_object.addColumn(FAMILY, ENDTIME, Bytes.toBytes(endTime));
			put_object.addColumn(FAMILY, KIND, Bytes.toBytes(kind));
			c.output(put_object);	
			}
		}
		catch(Exception e){
			e.printStackTrace(); 
			throw e;
		}

};
		
	

	public static void main(String[] args) 
	{
		// config object for writing to bigtable

		CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder().withProjectId("healthcare-12").withInstanceId("hc-dataset").withTableId("synpuf-data").build();

		// Start by defining the options for the pipeline.
		
		DataflowPipelineOptions  options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("healthcare-12");
		
		// The 'gs' URI means that this is a Google Cloud Storage path
		options.setStagingLocation("gs://synpuf_data/staging1");

		// Then create the pipeline.
		Pipeline p = Pipeline.create(options);
			CloudBigtableIO.initializeForWrite(p);
 		p.apply(TextIO.Read.from("gs://synpuf_data/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv")).apply(ParDo.named("Loading to Bigtable").of(MUTATION_TRANSFORM)).apply(CloudBigtableIO.writeToTable(config));
	
		p.run();

		//PCollection<String> lines=p.apply(TextIO.Read.from("gs://synpuf-data/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv"))
		//PCollection<String> fields = lines.apply(ParDo.of(new ExtractFieldsFn()));
		//p.apply(TextIO.Write.to("gs://synpuf-data/temp.txt"));
	}

}
