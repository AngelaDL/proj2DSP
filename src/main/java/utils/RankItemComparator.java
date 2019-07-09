package main.java.utils;

import java.util.Comparator;

public class RankItemComparator implements Comparator<RankItem> {

	@Override
	public int compare(RankItem r1, RankItem r2) {

		Long count1 = r1.getPopularity();
		Long count2 = r2.getPopularity();

		return -count1.compareTo(count2);
	}

}
