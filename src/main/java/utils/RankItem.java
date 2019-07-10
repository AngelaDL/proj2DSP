package main.java.utils;

import java.io.Serializable;
import java.util.Objects;

public class RankItem implements Serializable {

	private String articleID;
	private Long popularity;
	
	public RankItem(String articleID, Long popularity) {
		this.articleID = articleID;
		this.popularity = popularity;
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

	public Long getPopularity() {
		return this.popularity;
	}

	public String getArticleID() {
		return this.articleID;
	}
	
	@Override
	public String toString() {
		//return  this.getArticleID().concat(", ").concat(this.getPopularity().toString());
		return articleID + ":" + String.valueOf(popularity);
	}
}
