package com.example;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class RiskCalculator {/*
	public static void main(String[] args) 
	{
		DataflowPipelineOptions  options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("healthcare-12");
		options.setStagingLocation("gs://mihin-data/staging12");
		Pipeline p = Pipeline.create(options);
		PCollection<TableRow> weatherData = p.apply(
				BigQueryIO.Read
				.named("Read Patient Data")
				.from("healthcare-12:Mihin_Data_Sample.patientDataSet"));
		p.apply(TextIO.Read.named("Fetching File from Cloud").from("gs://healthcare-12/claims.csv")).apply(ParDo.named("Processing File").of(MUTATION_TRANSFORM))
		.apply(BigQueryIO.Write
				.named("Writeing to Big Querry")
				.to("healthcare-12:Mihin_Data_Sample.patientDataSet")
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
		p.run();

	}
	*/
}
