package b2_staging;

import b3_warehouse.WareHouse;

public class Main {
	
	public static void main(String[] args) {
//		String rs = args[0];
//		int id = Integer.parseInt(rs);
		LoadData load = new LoadData();
		
		load.loadFromSourceFile(1);
//		WareHouse wh = new WareHouse();
//		wh.transformToWareHouse(id);
	
	}
	
}
