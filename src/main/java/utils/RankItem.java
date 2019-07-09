package main.java.utils;

import java.io.Serializable;
import java.util.Objects;

public class RankItem implements Serializable {

	private static final long serialVersionUID = 1L;

	private String articleID;
	private long popularity;
	
	public RankItem() {
	}
	
	public RankItem(String articleID, long popularity) {
		super();
		this.articleID = articleID;
		this.popularity = popularity;
	}

	public long getPopularity() {
		return popularity;
	}

	public void setPopularity(long popularity) {
		this.popularity = popularity;
	}

	public String getArticleID() {
		return articleID;
	}

	public void setArticleID(String route) {
		this.articleID = route;
	}

	@Override
	public boolean equals(Object obj) {
	
		if (obj == null || !(obj instanceof RankItem))
			return false;
		
		RankItem other = (RankItem) obj;
		return Objects.equals(this.articleID, other.articleID);
		
		/*if (this.articleID.equals(other.articleID))
			return true;
		
		return false; */
	}
	
	@Override
	public String toString() {
		return  this.getArticleID().concat(", ").concat(String.valueOf(popularity));
		/*return articleID + ":" + String.valueOf(popularity); */
	}
}
