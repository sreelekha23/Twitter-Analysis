package com.practice;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;

@Entity
@Table(name="randomtable")
public class randomtable {
	
	@Id
	@Column(name="number")
	
	private int number;
	
	@Column(name="batchnumber", nullable=false)
	private String batchnumber;
	
	
	
	public int getNumber() {
		return number;
	}
	public void setNumber(int number) {
		this.number = number;
	}
	public String getBatchnumber() {
		return batchnumber;
	}
	public void setBatchnumber(String batchnumber) {
		this.batchnumber = batchnumber;
	}
	@Override
	public String toString() {
		return "randomtable [number=" + number + ", batchnumber=" + batchnumber + "]";
	}
	
	

}
