package oracle.demo.oow.bd.util.hbase;

import java.util.Iterator;
import java.util.List;

public class OperateList {
	
	public List<String> RemoveListValueRepeat(List<String> movieIdList){
		
		String obj[] = new String[movieIdList.size()];
		System.out.println(movieIdList.size());
		Iterator<String> iter = movieIdList.iterator();
		int n=0;
		
		while(iter.hasNext()){
			obj[n] = iter.next();
			n++;
		}
		movieIdList.clear();
		for(int i=0;i<n-1;i++){
			boolean isSingle = true;
			for(int j=i+1;j<n;j++){
				if(obj[i].equals(obj[j])){
					isSingle = false;
					break;
				}
			}
			if(isSingle){
				movieIdList.add(obj[i]);
			}
		}
		movieIdList.add(obj[n-1]);
		return movieIdList;
	}
}
