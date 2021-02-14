package com.hari.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SynchronizedDiskableMap<K, V> extends ConcurrentHashMap<K, V> implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final String FULL_CONTENT_STORAGE_FILE= System.getProperty("user.dir")+"/diskableMap.txt";
	
	private static final String TEMP_CONTENT_STORAGE_FILE = System.getProperty("user.dir") +"/diskableMapTemp.txt";

	// Max heap space in megabytes.
	private static final Long MAX_HEAP_SPACE = ((Runtime.getRuntime().totalMemory() / 1024) / 1024);

	// Specified limit to serialize objects to disk
	private Long writeToDiskHeapLimit = MAX_HEAP_SPACE;

	private Boolean hasWriteToDiskHeapLimitBreached = false;

	public SynchronizedDiskableMap() {
		super();
	}

	public SynchronizedDiskableMap(int intialCapacity, Long writeToDiskHeapLimit) {
		super(intialCapacity);
		this.writeToDiskHeapLimit = writeToDiskHeapLimit;
	}

	public SynchronizedDiskableMap(Map<? extends K, ? extends V> m) {
		super(m);
	}

	public SynchronizedDiskableMap(int initialCapacity, float loadFactor, Long writeToDiskHeapLimit) {
		super(initialCapacity, loadFactor);
		this.writeToDiskHeapLimit = writeToDiskHeapLimit;
	}

	public SynchronizedDiskableMap(int initialCapacity, float loadFactor, int concurrencyLevel, Long writeToDiskHeapLimit) {
		super(initialCapacity, loadFactor, concurrencyLevel);
		this.writeToDiskHeapLimit = writeToDiskHeapLimit;
	}

	@Override
	public V put(K key, V val) {
		if (!checkIfHeapLimitBreach()) {
			return super.put(key, val);
		} else {
			writeContentToDisk(this,FULL_CONTENT_STORAGE_FILE);
			setHasWriteToDiskHeapLimitBreached(true);
			return super.put(key, val);
		}
	}
	
	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		if (!checkIfHeapLimitBreach()) {
			super.putAll(m);
		} else {
			writeContentToDisk(this,FULL_CONTENT_STORAGE_FILE);
			setHasWriteToDiskHeapLimitBreached(true);
			super.putAll(m);
		}
	}
	
	@Override
	public V remove(Object key) {
		if(this.contains(key)) {
			return super.remove(key);
		}else if(hasWriteToDiskHeapLimitBreached){
			writeContentToDisk(this,TEMP_CONTENT_STORAGE_FILE);
			removeObjectFromDisk(key);
			reloadContentFromTemp();
			deleteTempFile();
		}
		return null;
	}
	
	private synchronized void reloadContentFromTemp() {
		ObjectInputStream objectinputstream = null;
		try {
			FileInputStream streamIn = new FileInputStream(TEMP_CONTENT_STORAGE_FILE);
			objectinputstream = new ObjectInputStream(streamIn);
			@SuppressWarnings("unchecked")
			Map<K, V> map = (Map<K, V>) objectinputstream.readObject();
			this.putAll(map);
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println("Exception in reading file:" + e.getMessage());
		} finally {
			try {
				objectinputstream.close();
			} catch (IOException e) {
				System.out.println("Unable to close the stream while reading object from file:" + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	private void deleteTempFile() {
		File file = new File(TEMP_CONTENT_STORAGE_FILE);
		file.delete();
	}

	@Override
	public V get(Object key) {
		if(this.containsKey(key)) {
			return this.get(key);
		}else{
			writeContentToDisk(this, TEMP_CONTENT_STORAGE_FILE);
			V value = findValueFromDisk(key);
			reloadContentFromTemp();
			return value;
		}
	}

	@SuppressWarnings("unchecked")
	private synchronized V findValueFromDisk(Object key) {
		ObjectInputStream objectinputstream = null;
		V valueFromFile = null;
		try {
			FileInputStream streamIn = new FileInputStream(FULL_CONTENT_STORAGE_FILE);
		    objectinputstream = new ObjectInputStream(streamIn);
		    Map<K, V> map = null;
		    do {
		        map = (Map<K, V>) objectinputstream.readObject();
		        if(map != null && map.containsKey(key)){
		        	valueFromFile = map.get(key);
		        } 
		    } while (map != null);
		}catch(IOException | ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println("Exception in reading file:"+e.getMessage());
		}finally {
			try {
				objectinputstream.close();
			} catch (IOException e) {
				System.out.println("Unable to close the stream while reading object from file:" + e.getMessage());
				e.printStackTrace();
			}
		}
		return valueFromFile;
	}

	@SuppressWarnings("unchecked")
	private synchronized void removeObjectFromDisk(Object key) {
		ObjectInputStream objectinputstream = null;
		try {
			FileInputStream streamIn = new FileInputStream(FULL_CONTENT_STORAGE_FILE);
		    objectinputstream = new ObjectInputStream(streamIn);
		    Map<K, V> map = null;
		    do {
		        map = (Map<K, V>) objectinputstream.readObject();
		        if(map != null && map.containsKey(key)){
		        	map.remove(key);
		        } 
		        writeContentToDisk(map,FULL_CONTENT_STORAGE_FILE);
		    } while (map != null);
		}catch(IOException | ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println("Exception in reading file:"+e.getMessage());
		}finally {
			try {
				objectinputstream.close();
			} catch (IOException e) {
				System.out.println("Unable to close the stream while reading object from file:" + e.getMessage());
				e.printStackTrace();
			}
		}
		     
	}

	private synchronized void writeContentToDisk(Map<K,V> map,String file) {
		ObjectOutputStream oos = null;
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(file);
			oos = new ObjectOutputStream(fos);
			oos.writeObject(map);
			map.clear();
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

	private boolean checkIfHeapLimitBreach() {
		return Runtime.getRuntime().freeMemory() < writeToDiskHeapLimit * 1024 * 1024;
	}
	
	public Boolean getHasWriteToDiskHeapLimitBreached() {
		return hasWriteToDiskHeapLimitBreached;
	}

	public void setHasWriteToDiskHeapLimitBreached(Boolean hasWriteToDiskHeapLimitBreached) {
		this.hasWriteToDiskHeapLimitBreached = hasWriteToDiskHeapLimitBreached;
	}
}
