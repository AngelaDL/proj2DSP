package main.java.utils;

import java.io.Serializable;
import java.util.ArrayList;

public class Ranking implements Serializable {

	private static final long serialVersionUID = 1L;

	private ArrayList<RankItem> ranking;
	
	public Ranking() {
	
	}

	public ArrayList<RankItem> getRanking() {
		return ranking;
	}

	public void setRanking(ArrayList<RankItem> ranking) {
		this.ranking = ranking;
	}
	
}
