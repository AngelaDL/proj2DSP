package main.java.utils;

import java.io.Serializable;
import java.util.Objects;

public class RankItem implements Serializable {

	private static final long serialVersionUID = 1L;

	private String articleID;
	private Long popularity;
	
	public RankItem(String articleID, Long popularity) {
		this.articleID = articleID;
		this.popularity = popularity;
	}

	public Long getPopularity() {
		return popularity;
	}

	public void setPopularity(long popularity) {
		this.popularity = popularity;
	}

	public String getArticleID() {
		return articleID;
	}

	public void setArticleID(String id) {
		this.articleID = id;
	}

	@Override
	public boolean equals(Object obj) {
	
		if (obj == null || !(obj instanceof RankItem))
			return false;
		
		RankItem other = (RankItem) obj;
		return Objects.equals(this.articleID, other.articleID);
	}
	
	@Override
	public String toString() {
		//return  this.getArticleID().concat(", ").concat(this.getPopularity().toString());
		return articleID + ":" + String.valueOf(popularity);
	}
}
