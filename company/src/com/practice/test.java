package com.practice;

import java.util.Scanner;

public class test {

	public static void main(String[] args) {
		randomtable r=new randomtable();
		generator g=new generator();
		System.out.println("enter the option");
		System.out.println("1.insert a new batch");
		System.out.println("2. retrieve the batch");
		
		Scanner sc=new Scanner(System.in);
		int input=sc.nextInt();
		switch(input){
		case 1: 
			
				System.out.println("enter the value for batch");
				int i=sc.nextInt();
				String batchNumber=generator.randomValue();
				System.out.println(batchNumber);
				r.setNumber(i);
				r.setBatchnumber(batchNumber);
				g.insert(r);
				break;
		
		
		case 2:
				
			System.out.println("enter the value to be Searched");
			int get=sc.nextInt();
			g.search(get);
			break;
	}
}
}
