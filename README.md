This program used for analysing the Ceiling Strategy

Data Sources:
  - Top of the book
  - csv file that contains possible ceiling days -> only used for efficiency it can be run without this csv


#There is options that you can change in the main.py before run the code
Options:
  - int_start_date: standas for start date 
  - int_end_date: standas for end date 
  - start_date: used in output file/directory generation (a bit misslabeled)
  - OUTPUT: select a actual path for report generation
  - PATH: select optional CSV path
  - checked: this flag stands for whether data that wanted to processed was entered first_day_checker before
  - evaluated: this flag stands for whether data that wanted to processed was entered open_position_evaluator before
  - spesific: this flag stands for whether you wanted to shrink the subset with spesific securities

Process:
  - main.py first calls pair generator which returns dataframe that contains directories
  - second it calls previous day properties class and obtains previous days closing price for being able to calculate current days ceiling price
  - then main calls first day checker and program decidces that if this security has reached ceiling that day, if it has checker also checks whether this position stopped
  - lastly open position evaluator have been called for deciding the what happened to the open position.
  - afterwards just report generation 