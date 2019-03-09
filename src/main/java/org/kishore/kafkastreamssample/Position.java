package org.kishore.kafkastreamssample;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

@Data
public class Position {
    private String position;
    private String dept;
    private String salary;

    public static void main(String[] args) throws JsonProcessingException {
        Position position = new Position();
        position.setDept("Head");
        position.setPosition("Lead");
        ObjectMapper mapper = new ObjectMapper();
        String s = mapper.writeValueAsString(position);
        System.out.println(s);

    }
}
