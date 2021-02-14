package com.hari.test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TestClass {

	public static void main(String[] args) {
		System.out.println((Runtime.getRuntime().totalMemory() / 1024) / 1024);
		System.out.println((Runtime.getRuntime().freeMemory() / 1024) / 1024);
		TestObject obj1 = new TestObject(1, "Hari");
		TestObject obj2 = new TestObject(2, "Dhana");
		Map<Integer, TestObject> testMap1 = new HashMap<Integer, TestObject>();
		testMap1.put(1, obj1);
		testMap1.put(2, obj2);
		writeContentToFile(testMap1);
		System.out.println(testMap1);
	}

	private static void writeContentToFile(Map<? extends Object, ? extends Object> map) {
		ObjectOutputStream oos = null;
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(System.getProperty("user.dir") + "/test.txt");
			oos = new ObjectOutputStream(fos);
			oos.writeObject(map);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Exception in writing object to file:" + e.getMessage());
		} finally {
			try {
				oos.close();
				fos.close();
			} catch (IOException e) {
				System.out.println("Unable to close the stream while writing object to file:" + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	private static class TestObject implements Serializable {
		private String name;

		private int id;

		public TestObject(int i, String string) {
			this.id = i;
			this.name = string;
		}
	}
}
