package com.nilge.hadoop.loadJsonData.property;

import org.apache.htrace.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.htrace.fasterxml.jackson.annotation.JsonProperty;
import org.apache.htrace.fasterxml.jackson.annotation.JsonTypeName;

import com.sun.tools.javac.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "business_id")
public class Business {
    @JsonIgnoreProperties("type")
    public String type;
    
    @JsonProperty("business_id")
    public String businessId;
    
    @JsonProperty("name")
    public String name;
    
    @JsonProperty("neighborhoods")
    public List<String> neighborhood;
    
    @JsonProperty("full_address")
    public String full_address;
    
    @JsonProperty("city")
    public String city;
    
    @JsonProperty("state")
    public String state;
    
    @JsonProperty("latitue")
    public String latitude;
    
    @JsonProperty("longitude")
    public String longitude;
    
    @JsonProperty("stars")
    public String stars;
    
    @JsonProperty("review_count")
    public String reviewCount;
    
    @JsonProperty("categories")
    public List<String> category;
    
    @JsonProperty("open")
    public boolean open;
    
    
}
