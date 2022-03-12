package org.flight.analysis.entity

case class Flight(year: String, month: String, day: String, dayOfWeek: String,
                  airline: String, flightNumber: String, tailNumber: String, originAirport: String,
                  destinationAirport: String, scheduledDeparture: String, departureTime: String,
                  departureDelay: String, taxiOut: String, wheelsOut: String, scheduledTime: String,
                  elapsedTime: String, airTime: String, distance: String, wheelsOn: String, taxiIn: String,
                  scheduledArrival: String, arrivalTime: String, arrivalDelay: String, diverted: String,
                  cancelled: String, cancellationsReason: String, airSystemDelay: String, securityDelay: String,
                  airlineDelay: String, lateAircraftDelay: String, weatherDelay: String)
