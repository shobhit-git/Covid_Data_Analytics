package com.amazonaws.covid.model;

import java.sql.Date;

public class CovidData {
	
	private Date date;
	private String country;
	private int cases;
	private int deaths;
	
	public Date getDate() {
		return date;
	}
	public void setDate(Date date) {
		this.date = date;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public int getCases() {
		return cases;
	}
	public void setCases(int cases) {
		this.cases = cases;
	}
	public int getDeaths() {
		return deaths;
	}
	public void setDeaths(int deaths) {
		this.deaths = deaths;
	}
	
	@Override
	public String toString() {
		return "CovidData [date=" + date + ", country=" + country + ", cases=" + cases + ", deaths="
				+ deaths + "]";
	}

}
