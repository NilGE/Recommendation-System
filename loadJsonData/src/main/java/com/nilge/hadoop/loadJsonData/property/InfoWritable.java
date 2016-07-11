package com.nilge.hadoop.loadJsonData.property;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class InfoWritable implements Writable {
    private String name;
    private String stars;
    
    public void readFields(DataInput in) throws IOException {
        name = in.readUTF();
        stars = in.readUTF();
    }
    
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(stars);
    }
    
    public void SetName(String name) {
        this.name = name;
    }
    
    public void SetStars(String stars) {
        this.stars = stars;
    }
    
    public String getName() {
        return name;
    }
    
    public String getStars() {
        return stars;
    }

    public boolean equals(Object o) {
        if (o instanceof InfoWritable) {
            InfoWritable info = (InfoWritable) o;
            return this.getName().equals(info.getName()) && this.getStars().equals(info.getStars());
        }
        return false;
    }
}
