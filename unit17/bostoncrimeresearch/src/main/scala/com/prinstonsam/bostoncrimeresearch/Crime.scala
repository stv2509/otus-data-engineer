package com.prinstonsam.bostoncrimeresearch

final case class Crime(

                        incidentNumber: String,//INCIDENT_NUMBER,
                        offenseCode: String,// OFFENSE_CODE,
/*
                        offenceCodeGroup: String,// OFFENSE_CODE_GROUP,
                        offenseDescription: String, // OFFENSE_DESCRIPTION,
*/
                        district: String,// DISTRICT,
/*
                        reportingArea: Int, // REPORTING_AREA,
                        shooting: String, // SHOOTING,
                        occuredOnDate: String, // OCCURRED_ON_DATE,
*/
                        year: Int,// YEAR,
                        month: Int, // MONTH,
/*
                        dayOfWeek: DayOfWeek, // DAY_OF_WEEK,
                        hour: Int, // HOUR,
                        ucrPart: String, // UCR_PART,
                        street: String, // STREET,
*/
                        lat: Double, // Lat,
                        lon: Double // Long,
//                        location: String// Location
                      )

//incident_number, offence_code, year, month, district, lat, long
