package com.amazonaws.covid;

import java.sql.Date;
import java.util.Random;

import com.amazonaws.covid.model.CovidData;

public class CovidStreamProducerUtility {

	public static void main(String[] args) {
		
		try {
			CovidStreamProducer.connectToAWS();
			CovidStreamProducer.createStream();
			CovidData data = null;
			//String data="";
			for(int i=0;i<=10;i++) {
				data = prepareCovidData("UnitedStates");
				CovidStreamProducer.putDataToStream(data);
				data = prepareCovidData("India");
				CovidStreamProducer.putDataToStream(data);
				data = prepareCovidData("China");
				CovidStreamProducer.putDataToStream(data);
				data = prepareCovidData("Italy");
				CovidStreamProducer.putDataToStream(data);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	private static CovidData prepareCovidData(String country) {
    	
    	CovidData data = new CovidData();
    	
    	data.setDate(new Date(System.currentTimeMillis()));
    	data.setCountry(country);
    	data.setCases(new Random().nextInt(5000));
    	data.setDeaths(new Random().nextInt(1000));
    	
    	return data;
    }
	
	private static String prepareCovidDataCSV(String country) {
    	
		int cases = new Random().nextInt(5000);
		int deaths = new Random().nextInt(1000);
		
    	String data = country+","+cases+","+deaths+"\n";
    	
    	return data;
    }

}
