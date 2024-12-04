# Import the dependencies.
import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import datetime as dt

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
        f"/api/v1.0/&lt;start&gt;<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    
    most_recent_date = session.query(func.max(measurement.date)).scalar()
    last_year = dt.datetime.strptime(most_recent_date, "%Y-%m-%d") - dt.timedelta(days=365)

    # Query the last 12 months of precipitation data
    precip_data = session.query(measurement.date, measurement.prcp)\
                         .filter(measurement.date >= last_year)\
                         .order_by(measurement.date.desc())\
                         .all()

    session.close()

    precip_dict = {date: prcp for date, prcp in precip_data if prcp is not None}
    return jsonify(precip_dict)

@app.route("/api/v1.0/tobs")
def tobs():
    most_recent_date = session.query(func.max(measurement.date)).scalar()
    last_year = dt.datetime.strptime(most_recent_date, "%Y-%m-%d") - dt.timedelta(days=365)

    # Query the last 12 months of temperature observation data (tobs)
    temp_data = session.query(measurement.date, measurement.tobs)\
                       .filter(measurement.date >= last_year)\
                       .order_by(measurement.date.desc())\
                       .all()

    session.close()

    tobs_list = [{"date": date, "tobs": tobs} for date, tobs in temp_data]
    return jsonify(tobs_list)

# Helper function to calculate temperature stats (tmin, tavg, tmax)
def calc_temps(start_date, end_date=None):

    print(f"Received request for start date: {start_date}, end date: {end_date}")

    session = Session(engine)
    
    # If an end date is provided, filter between start and end date
    if end_date:
        temps = session.query(func.min(measurement.tobs), 
                              func.avg(measurement.tobs), 
                              func.max(measurement.tobs))\
                       .filter(measurement.date >= start_date)\
                       .filter(measurement.date <= end_date).all()
    # If only the start date is provided, filter for dates >= start_date
    else:
        temps = session.query(func.min(measurement.tobs), 
                              func.avg(measurement.tobs), 
                              func.max(measurement.tobs))\
                       .filter(measurement.date >= start_date).all()

    session.close()
    return temps[0]

# Route for temperature stats based on start date only
@app.route("/api/v1.0/<start>")
def temp_stats_start(start):

    temps = calc_temps(start)
    
    temp_stats = {
        "Start Date": start,
        "TMIN": temps[0],
        "TAVG": temps[1],
        "TMAX": temps[2]
    }

    return jsonify(temp_stats)

# Route for temperature stats based on start and end date range
@app.route("/api/v1.0/<start>/<end>")
def temp_stats_range(start, end):

    temps = calc_temps(start, end)
    
    temp_stats = {
        "Start Date": start,
        "End Date": end,
        "TMIN": temps[0],
        "TAVG": temps[1],
        "TMAX": temps[2]
    }
    
    return jsonify(temp_stats)

# Define the main behavior
if __name__ == '__main__':
    app.run(debug=True)