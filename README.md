# NHL Data
This is a Python script that uses an API (https://gitlab.com/dword4/nhlapi/-/blob/master/stats-api.md) to collect ~50,000 rows of NHL player stats for every team and every season.

This involved a series of nested for loops:
- Loop through each team
- Loop through each season for that team
- Loop though each player on that team for that season and pull their stats

Each player's stats were appended to a main pandas dataframe. After all the data was retrieved, this dataframe was written to a CSV file. 

Subsequently in this project, the data in the CSV file was loaded into Microsoft SQL Server Studio using SSIS and conditional loops where SQL queries were made to answer a set of questions about the data.
