Supporting documentation

Task 1 assumptions
	- we only care about observations at a minute level
	- the unit of measure provided for the column value is in inches (the source is from the US which uses imperial units)
	- bad data is most likely considered to be over 12 as the highest recorded rainfall in an hour was 12''
	- data from the start of unixdatetime is valid (the rain gauage was invented in 1968 and the data could have been digitized along the way
	- bad data are observations in the future
	- for bad data we will filter out the rows and continue processing
	
Task 2
	- assumption that the data input is the supplied time range


	
Incremental load approach
The actual design of how the incremental load should work really depends on what needs to be achieved.
Likewise what are the possible errors that we can encounter and how does the business wants it handled.

Based on this I will be making the assumptions that
	- the data will be sourced only from a csv
	- the file represents the device that has uploaded
	- any errors encountered will not be loaded into a db but should be kept for investigation
	- the data can be loaded in batches
	- the data will be provided from the beginning of time and should not change historically unless it is in the rolling hour
	
Additional columns
- obeservation datetime format
	if the datetime format is not just unix this will provide the ability to handle it appropriately
- unit of measure (rainfall/value)
	given that rainfall is measured in more than one unit, we would need this to handle such appropriately
- device id
	this would be good for identifying the device uploading and possibly identifying different clients or lat ond long info etc.
	
How this would work - extending on "Incremental Load Diagram"
1.	get_source_data
		- be a control flow for the different sources of data (SQL db, API, CSV etc.)
		- maybe standardize the data format for the next step
2.  transform_source_data
		- apply transormations and data checks
		- maybe add another filter to just take the last 7 days data (to speed up the ETL)
		- if a data check has failed then (1) skip the file (2) alert users (3) move to a failed folder
3.	calculate_de_accumulate
		- another transformation, but I guess a specific step as part of the transformation process
4.	load to db
		- run a SP to truncate the extract table
		- load the data into the extract table
		- run another SP to handle the SQL db ETL side
5.	extract table
		- storage for batched load
6.	merge
		- merge statement for source and target table (Extract to Staging) on the key observation datetimekey and device id
7.	staging table
		- storage for everything that has been processed from its particular source, ready to process into a data mart or something of the like