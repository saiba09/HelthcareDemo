package com.example;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.opencsv.CSVParser;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
public class DataEntryPatient
{
	static final DoFn<String, TableRow> MUTATION_TRANSFORM = new DoFn<String, TableRow>() {
		private static final long serialVersionUID = 1L;
		@Override
		public void processElement(DoFn<String, TableRow>.ProcessContext c) throws Exception {
			String csvData = c.element();
			CSVParser csvParser = new CSVParser();
			String data[] = csvParser.parseLine(csvData);

			//	int age = 0;
			//			 Date today  = new Date();
			//		        Date d = new Date(data[2]);
			//		       age = ( today.getYear() - d.getYear() );
			TableRow row = new TableRow().set("patient_id", data[0]).set("name",data[1]).set("dateOfbirth",data[2]).set("provider_type",data[12]).set("prescription_count",Integer.parseInt(data[32]))
					.set("claim_type",data[4]).set("diagnosis_code",(data[5])).set("procedure_code",data[6]).set("place_of_service",data[8]).set("service_description",data[10]);
			c.output(row);
		}

	}; 
	public static void main(String[] args) 
	{
		DataflowPipelineOptions  options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("healthcare-12");
		options.setStagingLocation("gs://mihin-data/staging12");
		Pipeline p = Pipeline.create(options);
		p.apply(TextIO.Read.named("Fetching File from Cloud").from("gs://healthcare-12/GeneratedPatientData.csv")).apply(ParDo.named("Processing File").of(MUTATION_TRANSFORM))
		.apply(BigQueryIO.Write
				.named("Writeing to Big Querry")
				.to("healthcare-12:Mihin_Data_Sample.patientDataSet")
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
		p.run();

	}

}
