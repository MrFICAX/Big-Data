package models;

import lombok.*;
@Builder
@Getter
@Setter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor(force = true)
public class Location {
    final Double Latitude;
    final Double Longitude;
    final Double Altitude;
    final String   Label;
    final String User;
    final Integer Year;
    final Integer Month;
    final Integer Day;
    final Integer Hour;
    final Integer Minute;
    final Integer Second;
    final Integer   Timestamp;

    public Integer year(){
        return Year;
    }
}
