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
import com.util.Parser;
import com.dao.RiskFactor;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Count;
import java.io.IOException;
import java.text.SimpleDateFormat;
public class riskFactorFilUpload
{
	private static long row_id = 0;
	static final DoFn<String, TableRow> MUTATION_TRANSFORM = new DoFn<String, TableRow>() {
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
	public static void main(String[] args) 
	{
		DataflowPipelineOptions  options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("healthcare-12");
		options.setStagingLocation("gs://mihin-data/staging12");
		Pipeline p = Pipeline.create(options);
		p.apply(TextIO.Read.named("Fetching File from Cloud").from("gs://healthcare-12/Behavioral_Risk_Factor_Data__Heart_Disease___Stroke_Prevention.csv")).apply(ParDo.named("Processing File").of(MUTATION_TRANSFORM))
		.apply(BigQueryIO.Write
				.named("Writeing to Big Querry")
				.to("healthcare-12:health_care_data.Behavioral_Risk_Factor_Data__Heart_Disease")
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
		p.run();

	}

}
