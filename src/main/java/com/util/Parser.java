package com.util;
import com.opencsv.CSVParser;
import java.io.IOException;
import com.dao.*;
public class Parser {
	private CSVParser csvParser;
	public Parser() {
		// TODO Auto-generated constructor stub
		csvParser = new CSVParser();
	}
	public CitiesData getCityData(String csvData) throws IOException{
		String[] data = csvParser.parseLine(csvData);
		CitiesData cityDataObject = new CitiesData();
		int year = 0 ;
		if (! data[0].isEmpty()) {
			year= Integer.parseInt(data[0]) ;
		}
		String state = data[2];
		String category = data[6] ;
		String measure = data[8] ;

		
		float data_value1 = 0;
		if (! data[12].isEmpty()) {
			data_value1 = Float.parseFloat(data[12]);
		}
		int data_value = (int) data_value1;
		String low_confidence_Limit = data[13];
		String high_confidence_Limit = data[14] ;
		int population = 0 ;
		if (! data[17].isEmpty()) {
		population= Integer.parseInt(data[17]) ;
		}
		String issue = data[23];
		cityDataObject.setCategory(category);
		cityDataObject.setData_value(data_value);
		cityDataObject.setHigh_confidence_Limit(high_confidence_Limit);
		cityDataObject.setIssue(issue);
		cityDataObject.setLow_confidence_Limit(low_confidence_Limit);
		cityDataObject.setYear(year);
		cityDataObject.setMeasure(measure);
		cityDataObject.setPopulation(population);
		cityDataObject.setState(state);
		return cityDataObject;
	}
	public RiskFactor getRiskFactorData(String csvData) throws IOException{
		String[] data = csvParser.parseLine(csvData);
		RiskFactor riskFactorObject = new RiskFactor();
		int Year = 0 ;
		if (! data[0].isEmpty()) {
			Year= Integer.parseInt(data[0]) ;
		}
		String Location = data[2];
		String Category = data[8] ;
		String Topic = data[9] ;
		riskFactorObject.setCategory(Category);
		riskFactorObject.setLocation(Location);
		riskFactorObject.setTopic(Topic);
		riskFactorObject.setYear(Year);
		return riskFactorObject;
	}
}
