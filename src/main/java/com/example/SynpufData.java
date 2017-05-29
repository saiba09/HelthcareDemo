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
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.type.Date;
import com.opencsv.CSVParser;
import com.util.Parser;
import com.dao.RiskFactor;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Count;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
public class SynpufData
{
	private static long row_id = 0;
	final static TupleTag<String[]> tag1 = new TupleTag<String[]>();
	final static  TupleTag<String> tag2 = new TupleTag<String>();
	static final DoFn<String, TableRow> MUTATION_TRANS12FORM = new DoFn<String, TableRow>() {
		private static final long serialVersionUID = 1L;
		@Override
		public void processElement(DoFn<String, TableRow>.ProcessContext c) throws Exception {
			String csvData = c.element();
			Parser parser = new Parser();
			RiskFactor riskFactorObject = parser.getRiskFactorData(csvData);
			TableRow row = new TableRow().set("Year", riskFactorObject.getYear()).set("Location",riskFactorObject.getLocation()).set("Category",riskFactorObject.getCategory())
					.set("Topic",riskFactorObject.getTopic());
			c.output(row);
		}

	};
	static final DoFn<String, KV<String, String[]>> SUMMARY_TRANSFORM = new DoFn<String, KV<String , String[]>>() {
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(DoFn<String, KV<String, String[]>>.ProcessContext c) throws Exception {
			// TODO Auto-generated method stub
			String csvData = c.element();
			CSVParser parser = new CSVParser();
			String[] data = parser.parseLine(csvData);
			String key = data[0];
			String[] value = new String[5];
			value[0] = data[1]; // date of birth
			value[1] = data[5]; // ESRD
			value[2] = data[6]; // state code
			if(data[12].equals("1") || data[13].equals("1") || data[14].equals("1")||data[15].equals("1")||data[16].equals("1")||data[17].equals("1")||data[18].equals("1")||data[19].equals("1")||data[20].equals("1")||data[21].equals("1")||data[22].equals("1") ){
				value[4] = "true"; // chronic disease present or not
			}
			value[5] = data[15].equals("1") ? "true" : "false"; // cancer present or not
			KV<String, String[]> map =KV.of(key, value);
			c.output(map);
		}


	};
	static final DoFn<String,KV<String, String>> INPATIENT_TRANSFORM = new DoFn<String,KV<String, String>>() {
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(DoFn<String,KV<String, String>>.ProcessContext c) throws Exception {
			// TODO Auto-generated method stub
			String csvData = c.element();
			CSVParser parser = new CSVParser();
			String[] data = parser.parseLine(csvData);
			String key = data[0];
			String value = data[1];


			KV<String, String> map =KV.of(key, value);
			c.output(map);
		}


	};
	
	public static void main(String[] args) 
	{
		DataflowPipelineOptions  options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("healthcare-12");
		options.setStagingLocation("gs://mihin-data/staging12");
		Pipeline p = Pipeline.create(options);
		PCollection<String> benneficiarySummaryFile = p.apply(TextIO.Read.named("Fetching File from Cloud").from("gs://healthcare-12/Behavioral_Risk_Factor_Data__Heart_Disease___Stroke_Prevention.csv"));
		PCollection<String> inPatientClaimsFile = p.apply(TextIO.Read.named("Fetching File from Cloud").from("gs://healthcare-12/Behavioral_Risk_Factor_Data__Heart_Disease___Stroke_Prevention.csv"));
		PCollection<KV<String,String[]>> patientDetailsFromBS = benneficiarySummaryFile.apply(ParDo.named("Processing File").of(SUMMARY_TRANSFORM));
		PCollection<KV<String,String>> patientDetailsFromIP = inPatientClaimsFile.apply(ParDo.named("Processing File").of(INPATIENT_TRANSFORM));
		
		PCollection<KV<String, CoGbkResult>> coGbkResultCollection =KeyedPCollectionTuple.of(tag1, patientDetailsFromBS).and(tag2, patientDetailsFromIP)
			                         .apply(CoGroupByKey.<String>create());
		
		
		PCollection<TableRow> finalResultCollection =
			    coGbkResultCollection.apply(ParDo.of(
			      new DoFn<KV<String, CoGbkResult>, TableRow>() {
			        @Override
			        public void processElement(ProcessContext c) {
			          KV<String, CoGbkResult> e = c.element();Iterable<String> claims = e.getValue().getAll(tag2);
					    int count = 0 , age =0; 
					    Calendar today = Calendar.getInstance();
					    String patient_id, state_code = null,chronic_cond;
						boolean esrd = false;
					    boolean chorinic_disease_present = false , cancer_present = false ;
						for (String string : claims) {
							count++;
						}
						Iterable<String[]> summary = e.getValue().getAll(tag1);
						for (String[] string : summary) {
							state_code = string[2];
							age =today.getWeekYear() - Integer.parseInt(string[1].substring(0, 4));
							if (string[1].equals("Y")) {
								esrd = false;
							}
				
							chorinic_disease_present = Boolean.getBoolean(string[4]);
							cancer_present = Boolean.getBoolean(string[4]);
						}
						TableRow row = new TableRow().set("patient_id", e.getKey()).set("age",age).set("state_code",state_code)
								.set("esrd", esrd).set("chorinic_disease_present", chorinic_disease_present).set("no_of_times_patient_visited" , count)
								.set("cancer_present",cancer_present);
						c.output(row);
					}


				}));
				
		//		.apply(ParDo.named("Processing File").of(MUTATION_TRANSFORM))
		finalResultCollection.apply(BigQueryIO.Write
						.named("Writeing to Big Querry")
						.to("healthcare-12:synpuf_data.synpufData")
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
				p.run();

	}

}