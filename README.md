# Reykavik bus delay prediction

## Extracting bus delay from historical data

To extract the bus delay from the historical data, we used the following steps:

1. We have been storing bus data from straeto.is for a while now. We have a database with all the bus data since september 2023, which is roughly 2,5 months of data, and 40 million rows of data up to the time of writing. 

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

To extract the delay was difficult because there is a lot of info that has to be deduced from the data. For example, the bus can be stopped at a station, but it can also be stopped at a red light. The bus can be moving, but it can also be moving very slowly because of traffic. The bus can be stopped at a station, but it can also be stopped at a station because it is waiting for the next scheduled departure time.

To extract the delay, we can either look at each specific stop and to try to see how late the bus is for each individual stop, or to look at how late the bus is at the end of the route. 

To find out how late the bus is at each stop, we'll have to look at the scheduled departure time for each stop, and compare it to the actual departure time. This is difficult because there is never an exakt departure time. 