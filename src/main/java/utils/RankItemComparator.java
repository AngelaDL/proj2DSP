package main.java.utils;

import java.util.Comparator;

public class RankItemComparator implements Comparator<RankItem> {

	@Override
	public int compare(RankItem o1, RankItem o2) {
		
		if (o1.getPopularity() == o2.getPopularity()
				&& !o1.getArticleID().equals(o2.getArticleID())){
			//return - (int) (o1.getTimestamp() - o2.getTimestamp());
		}
		
		//return -(o1.getPopularity() - o2.getPopularity());
		return 0;
	}

}
