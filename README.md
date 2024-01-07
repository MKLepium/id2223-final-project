# Reykavik bus delay prediction

## Extracting bus delay from historical data

To extract the bus delay from the historical data, we used the following steps:

1. We have been storing bus data from straeto.is for a while now. We have a database with all the bus data since september 2023, which is roughly 3 months of data, and 40 million rows of data up to the time of writing. 

2. The data consists of:

- **time**: timestamp from GPS in format YYMMDDhhmmss
- **lat**, **lon**: vehicle position from GPS
- **head**: vehicle heading according to GPS in degrees (0=N)
- **fix**: GPS fix. 0=bad or no reliability for GPS info, 2=high quality, 1= lower quality
- **route**: bus line number
- **stop**: ID of last station visited
- **next**: ID of next station to be visited
- **code**: can be:
    - 2: vehicle stopped
    - 3: vehicle started moving
    - 4: vehicle power is off (message generated less frequently in this state)
    - 5: vehicle power was switched on
    - 6: timer triggered
    - 7: vehicle arrived or passed a station
- **dev**: device ID
- **fer**: ferry ID

## Feature Engineering

The first step to our projectt was to extract the delay from the data.
In order to do so, we needed a way to know when the bus was stopped at a station.
We knew when a bus was stopped at a station because stop/next would change.
We could therefore extract between which timestamps the bus was stopped at a station.

The next problem was identifying when the bus was scheduled to be at said station.
The gtfs (General Transit Feed Specification) data from straeto.is contains the scheduled departure time for each stop.
The problem is that the gtfs data contains multiple routes, and each route has multiple stops.
The stop ID is not unique, so we had to use the stop ID to make a look up in the schedule data to find the scheduled departure time for each stop.
This can be inaccurate at times. 
But a rough estimate is better than nothing.
We are also in contact with a straeto employee to get a more accurate solution.
But as of writing this, we have not come up with a better solution.


### Discussions on Delay "cutoff"

We wanted to include that we had a discussion on how to define a delay.
We had a few options:

1. A delay is every time a bus is not on time. If it is 1 second late, it is a delay.
2. Define a cutoff time. If the bus is less than N minutes late. It is not a delay. 

Currently we are using option 1, simply due to a lack of time. 
We do however have a prepared solution for option 2, which needs to be tested (train a model and compare the results).

## Model training

We are using a simple random forest regressor to predict the delay.
We are using the following features:

- **time**: timestamp from GPS in format YYMMDDhhmmss
- **weekday**: day of the week (hot encoded)
- **temp_min**: minimum temperature for the day
- **temp_max**: maximum temperature for the day
- **temp_avg**: average temperature for the day
- **fer**: ferry ID (hot encoded)
- **delay_no_cutoff**: delay in seconds (no cutoff)

Part of the project proposal answer from Jim was that we are supposed to "create point-in-time correct training data with no data leakage from Posgres"
This was a bit challenging but since we have a limited project scope, we decided to use the following approach:

We have a database with all the bus data since september 2023, which is roughly 3 months of data, and 40 million rows of data up to the time of writing.
We employed a 5-fold K-Fold cross-validation approach, enhancing the model’s robustness by mitigating dependency on any specific data split and providing a more generalized performance metric.

This is the best solution we could come up with, given the time constraints. 

## Results

The final product is a web application that allows the user to select a bus line, and it predicts the delay for the upcoming day.
You can also use a button to calculate the total delay of all bus lines for the upcoming day.

The web application is hosted on heroku, and can be found [here](http://88.99.215.78:8090/).
