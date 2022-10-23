There are 2 files in the dataset, offering_cleaned.txt and review_cleaned.txt. These files are the cleaned version of the hotel-review datasets crawled from TripAdvisor by CMU (https://www.cs.cmu.edu/~jiweil/html/hotel-review.html)

The first file contains the information of ~4,000 hotels, including the hotel class, the id of the hotel, the location, the name, and the region id.
The hotel class is the only column that might have a null value in the dataset and please only include the hotels that have a hotel class for all of the analyses in this homework. (Do not drop the null values from the dataset directly, instead, please use only SparkSQL query to filter out the null values).

The second file contains ~600,000 reviews for these hotels from TripAdvisor. Each row in the file contains the id of the author, the total number of helpful votes the author received previously, the username of the author, the date, the id of the review, the number of helpful votes of the review, the id of the hotel, the overall rating, the review text, and the review title.

Please try to print out the schema of the tables before you start to work on the questions. You may also use .show(5) to print out some rows to help to understand the structure of the dataset.