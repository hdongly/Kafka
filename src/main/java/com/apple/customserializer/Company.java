package com.apple.customserializer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Company {
    public String name;
    public String brand;

    public static Company of(String name, String brand) {
        return new Company(name, brand);
    }
}
