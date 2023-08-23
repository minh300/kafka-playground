package org.melp.datastore.model;

import java.util.Objects;
import java.util.UUID;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity(name="business")
public class Business {
	@Id
	private String business_id;
	private String name;
	private String address;
	private String city;
	private String state;
	private String postal_code;
	private float latitude;
	private float longitude;
	private float stars;
	private int review_count;
	private int is_open;
	private String categories;

	public Business() {
	}

	public Business(final String name, final String address, final String city,
			final String state, final String postal_code, final float latitude,
			final float longitude, final float stars, final int review_count,
			final int is_open, final String categories) {
		this.name = name;
		this.address = address;
		this.city = city;
		this.state = state;
		this.postal_code = postal_code;
		this.latitude = latitude;
		this.longitude = longitude;
		this.stars = stars;
		this.review_count = review_count;
		this.is_open = is_open;
		this.categories = categories;
		this.business_id = UUID.randomUUID().toString().replace("-", "").substring(0,22);
	}
	
	public String getBusiness_id() {
		return business_id;
	}

	public void setBusiness_id(String business_id) {
		this.business_id = business_id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getPostal_code() {
		return postal_code;
	}

	public void setPostal_code(String postal_code) {
		this.postal_code = postal_code;
	}

	public float getLatitude() {
		return latitude;
	}

	public void setLatitude(float latitude) {
		this.latitude = latitude;
	}

	public float getLongitude() {
		return longitude;
	}

	public void setLongitude(float longitude) {
		this.longitude = longitude;
	}

	public float getStars() {
		return stars;
	}

	public void setStars(float stars) {
		this.stars = stars;
	}

	public int getReview_count() {
		return review_count;
	}

	public void setReview_count(int review_count) {
		this.review_count = review_count;
	}

	public int getIs_open() {
		return is_open;
	}

	public void setIs_open(int is_open) {
		this.is_open = is_open;
	}

	public String getCategories() {
		return categories;
	}

	public void setCategories(String categories) {
		this.categories = categories;
	}


	@Override
	public boolean equals(Object o) {

		if (this == o)
			return true;
		if (!(o instanceof Business))
			return false;
		Business business = (Business) o;
		return Objects.equals(this.business_id, business.business_id)
				&& Objects.equals(this.name, business.name)
				&& Objects.equals(this.address, business.address)
				&& Objects.equals(this.city, business.city)
				&& Objects.equals(this.state, business.state)
				&& Objects.equals(this.postal_code, business.postal_code)
				&& Objects.equals(this.latitude, business.latitude)
				&& Objects.equals(this.stars, business.stars)
				&& Objects.equals(this.review_count, business.review_count)
				&& Objects.equals(this.is_open, business.is_open)
				&& Objects.equals(this.categories, business.categories);
	}

	@Override
	public int hashCode() {
		return Objects.hash(business_id, name, address, city, state, postal_code, latitude, stars, review_count, is_open, categories);
	}

	@Override
	public String toString() {
		return "Business{" + "business_id=" + business_id + ", name='" + name + '\''
				+ ", address='" + this.address + '\'' + '}';
	}
}
