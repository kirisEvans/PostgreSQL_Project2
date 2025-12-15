package io.sustc.dto;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The user registration request information class
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RegisterUserReq implements Serializable {

    private String password;

    private String name;

    private Gender gender;

    private String birthday;

    public enum Gender {
        MALE,
        FEMALE,
        UNKNOWN,
    }
}
