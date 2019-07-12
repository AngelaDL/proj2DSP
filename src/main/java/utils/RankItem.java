package main.java.utils;

import java.io.Serializable;

public class RankItem implements Serializable {

	private String articleID;
	private long popularity;
	private long timestamp;

	public RankItem() {

	}

	public RankItem(String articleID, long popularity, long timestamp) {
		super();
		this.articleID = articleID;
		this.popularity = popularity;
		this.timestamp = timestamp;
	}

	@Override
	public boolean equals(Object obj) {

		if (obj == null || !(obj instanceof RankItem))
			return false;

		RankItem other = (RankItem) obj;

        if (this.articleID.equals(other.articleID))
            return true;

        return false;
	}

	public long getPopularity() {
		return this.popularity;
	}

	public String getArticleID() {
		return this.articleID;
	}

	public long getTimestamp() {
		return this.timestamp;
	}
	
	@Override
	public String toString() {
		//return  this.getArticleID().concat(", ").concat(this.getPopularity().toString());
		return this.getArticleID() + ":" + (String.valueOf(this.getPopularity()));
	}
}
