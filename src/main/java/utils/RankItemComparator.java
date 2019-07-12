package main.java.utils;

import java.util.Comparator;

public class RankItemComparator implements Comparator<RankItem> {

	@Override
	public int compare(RankItem r1, RankItem r2) {

		/*Long count1 = r1.getPopularity();
		Long count2 = r2.getPopularity();

		return -count1.compareTo(count2);*/

		if (r1.getPopularity() == r2.getPopularity()
				&& !r1.getArticleID().equals(r2.getArticleID())) {
			return - (int) (r1.getTimestamp() - r2.getTimestamp());
		}

		return - (int) (r1.getPopularity() - r2.getPopularity());
	}

}
