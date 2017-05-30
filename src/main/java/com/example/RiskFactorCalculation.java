package com.example;

import com.google.api.services.bigquery.model.TableRow;

public class RiskFactorCalculation {
public TableRow calculateScore(TableRow row){
	int agePoint = getAgePoints(row.get("Age").toString());
	int hospiPoints = getHospitalizationPoints(Integer.parseInt(row.get("hospitalizationCount").toString()));
	int erVisitPoints = getERVisitPoints(Integer.parseInt(row.get("erVisitCount").toString()));
	int severeDigPoints = getSevereDiagnosisPoints(Integer.parseInt(row.get("severeDiagnosisCount").toString()));
	int comorbidDigPoints = getComorbidDiagnosisPoints(Integer.parseInt(row.get("comorbidDiagnosisCount").toString()));
	int uniquePrescriptionPoints = getUniquePrescriptionPoints(Integer.parseInt(row.get("uniquePrescriptionCount").toString()));
	int hosiWithComorbidConPoints = getComorbidHopitalizationPoints(Integer.parseInt(row.get("hospitwithComorbidConCount").toString()));
	int erVisitWithComorbidPoints = getERVisitsWithComorbidPoints(Integer.parseInt(row.get("erVisitwithComorbidConCount").toString()));
	int canserPoint = getCancerPoints(Boolean.parseBoolean(row.get("isCancer").toString()));
	int score = agePoint + hospiPoints + erVisitPoints + severeDigPoints +comorbidDigPoints + uniquePrescriptionPoints + hosiWithComorbidConPoints
			+  erVisitWithComorbidPoints + canserPoint;
	int riskLevel = 0;
	String level = "";
	if(score <=2) {
		riskLevel = 0;
		level  = "negligible";
	}
	else if(score >= 3 && score <=6 ) {
		riskLevel = 1;
		level = "low";
	}
	else if(score >= 7 && score <=10 ) {
		riskLevel = 2;
		level = "moderate";
	}
	else if(score >= 11 && score <= 20 ) {
		riskLevel = 3;
		level = "high";
	}
	row.set("riskLevel", riskLevel);
	row.set("level" , level);
	return row;
}
public int getAgePoints(String str) {
	int age = Integer.valueOf(str);
	int point = 0;
	if(age>=65 && age < 80) {
		point = 1;
	} else if(age >=80) {
		point = 2;
	}
	return point;
}

public int getHospitalizationPoints(int noOfDay) {
	int points = 0;
	if(noOfDay >=1 && noOfDay <=4) {
		points = 1;
	} if(noOfDay > 4 ) {
		points =2;
	}
	return points;
}

public int getERVisitPoints(int count) {
	int points = 0;
	if(count >=1 && count <=5) {
		points = 1;
	} else if( count >5) {
		points = 2;
	}
	return points;
}


public int getCancerPoints(boolean isCancer) {
	int points = 0;
	if(isCancer) {
		points =5;
	}
	return points;
}

public int getERVisitsWithComorbidPoints(int counts) {
	int points =counts;
	return points;
}

public int getComorbidHopitalizationPoints(int counts) {
	int points =counts;
	return points;
}
public int getUniquePrescriptionPoints(int count) {
	int points =0;

	if(count >= 4 && count <= 6) {
		points =1;
	} else if(count>6) {
		points = 2;
	}

	return points;
}

public int getSevereDiagnosisPoints (int count) {
	int points =0;
	if(count >= 1 && count <= 3) {
		points =1;
	} else if(count>2) {
		points = 2;
	}
	return points;
}

public int getComorbidDiagnosisPoints (int count) {
	int points =0;
	if(count >= 1 && count <= 3) {
		points =1;
	} else if(count>2) {
		points = 2;
	}
	return points;
}
}
