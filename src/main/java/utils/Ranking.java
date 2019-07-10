package main.java.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Ranking implements Serializable {

	private List<RankItem> ranking;

	public List<RankItem> getRanking() {
		return ranking;
	}

	public void setRanking(List<RankItem> ranking) {
		this.ranking = ranking;
	}
	
}
