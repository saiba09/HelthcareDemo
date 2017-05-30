package com.example;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

public class RiskCalculator {
	final static TupleTag<String[]> tag1 = new TupleTag<String[]>();
	final static  TupleTag<String[]> tag2 = new TupleTag<String[]>();
	static final DoFn<TableRow, KV<String , String[]>> MAPREDUCETRANSFORM = new DoFn<TableRow,  KV<String , String[]>>() {
		private static final long serialVersionUID = 1L;
		@Override
		public void processElement(DoFn<TableRow,  KV<String , String[]>>.ProcessContext c) throws Exception {
			TableRow tableRow = c.element();
			String columns[] = new String[3];
			columns[0] = (String) tableRow.get("prescription_count");
			columns[1] = (String) tableRow.get("place_of_service");
			columns[2] = (String) tableRow.get("diagnosis_code");
			KV<String , String[]> patientRow =KV.of(tableRow.get("patient_id").toString(), columns);
			c.output(patientRow);
		}

	}; 
	static final DoFn<TableRow, KV<String , String[]>> FLATENINGTRANSFORM = new DoFn<TableRow,  KV<String , String[]>>() {
		private static final long serialVersionUID = 1L;
		@Override
		public void processElement(DoFn<TableRow,  KV<String , String[]>>.ProcessContext c) throws Exception {
			TableRow tableRow = c.element();
			String columns[] = new String[2];
			columns[0] = (String) tableRow.get("no_of_visit");
			columns[1] = (String) tableRow.get("Age");
			KV<String , String[]> patientRow =KV.of(tableRow.get("patient_id").toString(), columns);
			c.output(patientRow);
		}

	}; 
	static final DoFn<KV<String, Iterable<String[]>>,  KV<String , String[]>> AGGEGRATIONTRANSFORM = new DoFn<KV<String, Iterable<String[]>>,  KV<String , String[]>>() {
		private static final long serialVersionUID = 1L;
		@Override
		public void processElement(DoFn<KV<String, Iterable<String[]>>,   KV<String , String[]>>.ProcessContext c) throws Exception {
			KV<String,Iterable<String[]>> inputPatient = c.element();
			Iterable<String[]> patient_details = inputPatient.getValue();
			String[] value = new String[7];
			int uniquePrescriptionCount = 0;
			int severeDiagnosisCount = 0;
			int erVisitCount = 0 , hospitwithComorbidConCount =0;
			 int comorbidDiagnosisCount = 0 , erVisitwithComorbidConCount = 0; 
			 boolean isCancer = false;
			for (String[] strings : patient_details) {
				uniquePrescriptionCount += Integer.parseInt(strings[0]);
				if (true) {
					//add ICD code decoding
					severeDiagnosisCount ++;
				}
				if (strings[1].equalsIgnoreCase("20")) {
					erVisitCount ++;
				}
				if (true) {
					comorbidDiagnosisCount++;
				} 
				if (strings[1].equalsIgnoreCase("11") &&  true) {
					hospitwithComorbidConCount++;
				}
				if (strings[1].equalsIgnoreCase("20")  &&  true) {
					erVisitwithComorbidConCount ++;
				}
				if (true) {
					isCancer = true;
				}
			}
			value[0] = "" + uniquePrescriptionCount ; //
			value[1] = "" + severeDiagnosisCount;
			value[2] = "" + erVisitCount;
			value[3] = "" + comorbidDiagnosisCount;
			value[4] = "" + hospitwithComorbidConCount;
			value[5] = "" + erVisitwithComorbidConCount;
			value[6] = "" + isCancer;
			KV<String , String[]> patientRow =KV.of(inputPatient.getKey().toString(), value);
			c.output(patientRow);
		}

	}; 
	public static void main(String[] args) 
	{
		DataflowPipelineOptions  options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("healthcare-12");
		options.setStagingLocation("gs://mihin-data/staging12");
		Pipeline p = Pipeline.create(options);
		PCollection<TableRow> AggregatedData = p.apply(BigQueryIO.Read.named("Aggregating Patient Data").fromQuery("SELECT patient_id,count( patient_id ) as no_of_visit ,YEAR(CURRENT_DATE()) - INTEGER( RIGHT( dateOfbirth , 4 ) ) AS Age "
				 + "FROM [healthcare-12:Mihin_Data_Sample.patientDataSet] GROUP BY patient_id , Age "));
		PCollection<TableRow> PatientData = p.apply(BigQueryIO.Read.named("Reading Patient Data").fromQuery("SELECT patient_id , prescription_count, "
				+ "place_of_service, diagnosis_code FROM [healthcare-12:Mihin_Data_Sample.patientDataSet] "));
		PCollection<KV<String, String[]>> patientDataMap = PatientData.apply(ParDo.named("Processing Data").of(MAPREDUCETRANSFORM));
		PCollection<KV<String, Iterable<String[]>>> groupedPatient = patientDataMap.apply(GroupByKey.<String, String[]>create());
		PCollection<KV<String, String[]>> transformedPatientData = groupedPatient.apply(ParDo.named("Extracting Values").of(AGGEGRATIONTRANSFORM));
		PCollection<KV<String, String[]>> flatenedAggregateData = AggregatedData.apply(ParDo.named("Processing Data").of(FLATENINGTRANSFORM));
		PCollection<KV<String, CoGbkResult>> coGbkResultCollection =KeyedPCollectionTuple.of(tag1, flatenedAggregateData).and(tag2, transformedPatientData)
				.apply(CoGroupByKey.<String>create());
		
		PCollection<TableRow> finalResultCollection =
				coGbkResultCollection.apply(ParDo.named("Processing into table format").of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
							@Override
							public void processElement(ProcessContext c) {
								KV<String, CoGbkResult> e = c.element();
								Iterable<String[]> claims = e.getValue().getAll(tag1);
								String patient_id = e.getKey().toString();
								int hospitalizationCount = 0 , age = 0;
								
								int uniquePrescriptionCount = 0;
								int severeDiagnosisCount = 0;
								int erVisitCount = 0 , hospitwithComorbidConCount =0;
								 int comorbidDiagnosisCount = 0 , erVisitwithComorbidConCount = 0; 
								 boolean isCancer = false;	
								 for (String string[] : claims) {
									hospitalizationCount =Integer.parseInt(string[0]);
									age = Integer.parseInt(string[1]);
								}
								Iterable<String[]> summary = e.getValue().getAll(tag2);
								for (String[] string : summary) {
									uniquePrescriptionCount = Integer.parseInt(string[0]);
									severeDiagnosisCount = Integer.parseInt(string[1]);
									erVisitCount = Integer.parseInt(string[2]);
									hospitwithComorbidConCount = Integer.parseInt(string[4]);
									comorbidDiagnosisCount = Integer.parseInt(string[3]);
									erVisitwithComorbidConCount = Integer.parseInt( string[5]);
									isCancer = Boolean.parseBoolean(string[6]);
								
								} 
								TableRow row = new TableRow().set("patient_id", patient_id).set("Age",age).set("hospitalizationCount",hospitalizationCount)
										.set("uniquePrescriptionCount", uniquePrescriptionCount).set("severeDiagnosisCount", severeDiagnosisCount).set("erVisitCount" , erVisitCount)
										.set("hospitwithComorbidConCount",hospitwithComorbidConCount).set("comorbidDiagnosisCount", comorbidDiagnosisCount)
										.set("erVisitwithComorbidConCount", erVisitwithComorbidConCount).set("isCancer", isCancer); 

								c.output(row);
							}


						}));
		PCollection<TableRow> riskFactorCollection = finalResultCollection.apply(ParDo.named("Calculating risk factor").of(new DoFn<TableRow, TableRow>() {
			@Override
			public void processElement(ProcessContext c) {
				
               RiskFactorCalculation riskfactorcalculation = new RiskFactorCalculation();
             TableRow resultRow =  riskfactorcalculation.calculateScore(c.element());
				c.output(resultRow);
			}
		}));
		riskFactorCollection.apply(BigQueryIO.Write.named("Writeing to Big Querry Storage")
				.to("healthcare-12:Mihin_Data_Sample.RiskFactorCalculation")
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
		p.run();

		/*
	.apply(ParDo.named("Processing File").of(MUTATION_TRANSFORM))
		finalResultCollection.apply(BigQueryIO.Write
				.named("Writeing to Big Querry")
				.to("healthcare-12:Mihin_Data_Sample.patientDataSet")
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
		p.run();*/

	}
	
}
