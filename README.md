### UNC Charlotte Silver Route Analysis
The goal of this project is to extract location data for the UNC Charlotte Bus System and perform an analysis of the routes, speeds, passenger load, and more to find trends and potential inefficiencies.
Tableau Dashboard: https://public.tableau.com/app/profile/darren.summerlee/viz/UNCCSilverBusDashboard/DwellTimePerStop

## Process
Location data was extracted using the PassioGo API created by Andrei Thüler. An AWS Lambda function collected minute-by-minute data from 8 a.m. to 8 p.m., from 3/4/26 to 5/9/26, which was stored in NeonDB. 
To find the location of the stops, a PostgreSQL query was used to find the most common coordinates of the buses, which was cross referenced with the PassioGo map to eliminate any non-stops (intersections, crosswalks, etc.). This is shown in the “Identify Silver Route Stops” sheet. <br>
Some stops occur in similar locations, but moving in different directions. To account for this, a PostgreSQL query was created to find the most common direction the buses were heading at the previously found stops, and these directions were attributed to the correct stops. For stops that are located on roads on which the bus only travels one way, the direction variable is given a generous 180-degree buffer, allowing for any glitches or odd angles at the moment of capture. For the stops that exist on a road for which the bus travels in both directions, the direction is given a strict 90-degree buffer (+/- 45 degrees).

## Analysis
The ‘Dwell Time Per Stop’ Dashboard shows that the CRI Deck is the stop dwelled at for the most total time, followed by Martin Hall. These stops likely act as resting points either for the drivers’ convenience, and/or to ensure that other buses along the route are well spaced. A similar pattern emerges at Student Union East and West, which serve as the midpoint between the Marin Hall stop and the CRI Deck stop. <br>
The ‘STUN Stops Dashboard’ Explores the distribution of stop time at the Student Union East and West stops across the hour of the day, and day of the week. Earlier in the day, and on weekends, the buses dwell at these resting points longer, likely due to the lack of ridership. When ridership is presumably higher in the middle of weekdays, no trend emerges. <br>
The ‘East/West Stops Dashboard’ Explores the Martin Hall and CRI Deck stops. The trend of more time spent in the mornings and on weekends continues in both of these stops. But during weekdays, the time spent at these stops clearly decreases as the day progresses, most notably with the CRI Deck stop. I hypothesize that when traffic is heavy due to the conclusion of classes, less time is spent at these stops. Traffic is often heaviest around these stops, as many students exit through the eastern or western roads. This hypothesis will be addressed in future work. <br>
Another stark trend seen in the ‘East/West Stops Dashboard’ is a high volume of time spent at the Martin Hall stop during the 1 p.m. hour, and a lack of time spent at the CRI Deck stop during the 2 p.m. hour.

## Future Work
The next step in this project is to categorize non-stops into the correct segments of the bus route that exists between stops. This will be done by creating a ruleset as such: if longitude is between x1 and x2 and latitude is between y1 and y2, and direction is between 1 and 2, the bus’s next stop is Stop A.
