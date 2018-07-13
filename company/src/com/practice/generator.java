package com.practice;
import java.io.Serializable;
import java.text.Annotation;
import java.util.Random;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.AnnotationConfiguration;
import org.hibernate.cfg.Configuration;



@SuppressWarnings("deprecation")
public class generator{
	
	private static Configuration configuration=null;
	private static SessionFactory sessionFactory=null;
	static {
		System.out.println("loading cfg");
		configuration= new AnnotationConfiguration();
		configuration.configure();
		sessionFactory= configuration.buildSessionFactory();
		System.out.println("loaded");
	}
	
	
	public static String randomValue() {
		
		String novowels = "BCDFGHJKLMNPQRSTVWXYZ";
		String base = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
		Random random = new Random();
		int max = 6;
		int min = 4;
		
		StringBuilder b = new StringBuilder();
		b.append(novowels.charAt(random.nextInt(novowels.length())));
		for (int j = 1; j < random.nextInt((max - min) + 1) + min; j++) {
		b.append(base.charAt(random.nextInt(base.length())));
		}
			return b.toString();
	}
	public boolean insert(randomtable r) {
		
		Session session=null;
		try {
			session= sessionFactory.openSession();
			Transaction transaction=session.beginTransaction();
			Serializable id= session.save(r);
			transaction.commit();
			System.out.println("empl id:"+id);
		}catch(HibernateException e){
			e.printStackTrace();
		}finally {
			try {
				if(session!=null) {
					session.close();
				}
			}catch(HibernateException e){
				e.printStackTrace();
			}
		}
		return false;
		
	}
	
	public void search(int number) {
		Session session= null;
		try {
			session= sessionFactory.openSession();
			randomtable r=(randomtable)session.load(randomtable.class, number);
			
			System.out.println(r);
		}finally {
			try {
				if(session!=null) {
					session.close();
				}
			}catch(HibernateException e){
				e.printStackTrace();
			}
		}
	}
}	


