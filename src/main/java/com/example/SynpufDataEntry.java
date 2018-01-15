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
import com.opencsv.CSVParser;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
public class SynpufDataEntry
{
	static final DoFn<String, TableRow> MUTATION_TRANSFORM = new DoFn<String, TableRow>() {
		private static final long serialVersionUID = 1L;
		@Override
		public void processElement(DoFn<String, TableRow>.ProcessContext c) throws Exception {
			String line = c.element();
			CSVParser csvParser = new CSVParser();
			String[] parts = csvParser.parseLine(line);	
			TableRow row = new TableRow().set("DESYNPUF_ID", parts[0]).set("BENE_SEX_IDENT_CD", parts[3]).set("SP_ALZHDMTA",parts[12]).set("SP_CHF",parts[13]).set("SP_DEPRESSN",parts[17]).set("SP_CHRNKIDN",parts[14]);
			c.output(row);
		}

	}; 
	public static void main(String[] args) 
	{
		DataflowPipelineOptions  options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("dummyproject-05042017");
		options.setStagingLocation("gs://healthcare_demo_stagging/staging12");
		Pipeline p = Pipeline.create(options);
		p.apply(TextIO.Read.from("gs://healthcare-demo/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv")).apply(ParDo.named("Loading to BigQuery").of(MUTATION_TRANSFORM))
		.apply(BigQueryIO.Write
				.named("Write")
				.to("dummyproject-05042017:healthcare_demo.Synpuf_data_bene_summary")
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
		p.run();
	}

}