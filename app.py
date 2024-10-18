# Import the dependencies.
import numpy as np
import pandas as pd
import datetime as dt
from datetime import datetime, timedelta

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, desc

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
measurement = Base.classes.measurement
station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)



#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/2016-08-23<br/>"   #May break
        f"/api/v1.0/2016-08-23/2017-08-22"  #May break
    )

@app.route("/api/v1.0/precipitation")
def last_year_precipitation():
    # Create Dates for last 12 months of the data
    end_date_str = session.query(func.max(measurement.date)).scalar()
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    start_date = end_date - timedelta(days=365)

    # Query last 12 months utilizing dates from above
    results = session.query(
        measurement.date,
        measurement.prcp
    ).filter(
        (func.strftime("%Y-%m-%d", measurement.date) >= start_date), 
        (func.strftime("%Y-%m-%d", measurement.date) <= end_date)
    ).all()

    session.close()

    # Create a dictionary from row data and append to a list of all precipitation data
    last_year = []
    for date, prcp in results:
        prcp_dict = {}
        prcp_dict["date"] = date
        prcp_dict["prcp"] = prcp
        last_year.append(prcp_dict)

    return jsonify(last_year)

@app.route("/api/v1.0/stations")
def all_stations():
    # Query station list
    results = session.query(station.station).all()

    session.close()

    # Convert list of tuples into normal list
    all_stations = list(np.ravel(results))

    return jsonify(all_stations)

@app.route("/api/v1.0/tobs")
def most_active_obs():
# Query last 12 months of data
    
    # Create Dates
    end_date_str = session.query(func.max(measurement.date)).scalar()
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    start_date = end_date - timedelta(days=365)
    
    # Select most active station
    active_station = session.query(
        measurement.station
    ).group_by(
        measurement.station
    ).order_by(
        desc(func.count(measurement.tobs))
    ).limit(1).scalar()
    
    #Create 12 month for most active station query
    results = session.query(
        measurement.date,
        measurement.tobs
    ).filter(
        (measurement.station == active_station),
        (func.strftime("%Y-%m-%d", measurement.date) >= start_date), 
        (func.strftime("%Y-%m-%d", measurement.date) <= end_date)
    ).all()

    session.close()

# Create a dictionary for most active query results
    active_year = []
    for date, tobs in results:
        tobs_dict = {}
        tobs_dict["date"] = date
        tobs_dict["tobs"] = tobs
        active_year.append(tobs_dict)

# Jsonify data to be returned
    return jsonify(active_year)


@app.route("/api/v1.0/<start>")
@app.route("/api/v1.0/<start>/<end>")
def temp_stats(start, end=None):
    """Fetch the minimum, maximum, and average temperature from 
       the given start date through a given end date, if provided,
       otherwise the last date in the dataset."""

    start_date = datetime.strptime(start, '%Y-%m-%d')

    if end is None:
        end = '9999-12-31'  #End date if none is given to ensure all data included
    end_date = datetime.strptime(end, '%Y-%m-%d')

# Query data from selected start date from user.    
    results = session.query(
        func.min(measurement.tobs).label('lowest_temp'),
        func.max(measurement.tobs).label('highest_temp'),
        func.avg(measurement.tobs).label('avg_temp')
    ).filter(
        (func.strftime("%Y-%m-%d", measurement.date) >= start_date), 
        (func.strftime("%Y-%m-%d", measurement.date) <= end_date)
    ).one()

    session.close()

# Create a dictionary for stat query results
    stats_dict = {
        'lowest_temperature': results.lowest_temp,
        'highest_temperature': results.highest_temp,
        'average_temperature': results.avg_temp
    }

# Jsonify data to be returned    
    return jsonify(stats_dict)


if __name__ == "__main__":
    app.run(debug=True)