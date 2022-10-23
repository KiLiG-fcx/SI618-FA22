from pyspark import SparkContext
from pyspark.sql import SQLContext

if __name__ == "__main__":

    sc = SparkContext(appName="umsi618f22hw7")
    sqlc = SQLContext(sc)
    offer = sqlc.read.json("/scratch/siads618f22_class_root/siads618f22_class/shared_data/hw7_data/offering_cleaned.txt")
    offer.createOrReplaceTempView("offer")
    review = sqlc.read.json("/scratch/siads618f22_class_root/siads618f22_class/shared_data/hw7_data/review_cleaned.txt")
    review.createOrReplaceTempView("review")
    
    reviewhotel = sqlc.sql('''
    SELECT *, review.id as review_id FROM offer
    RIGHT OUTER JOIN review
    ON review.offering_id=offer.id
    WHERE offer.hotel_class IS NOT NULL
    ''')
    reviewhotel.createOrReplaceTempView("reviewhotel")
    avgrating = sqlc.sql('''
    SELECT offering_id AS hotel_id, AVG(ratings_overall) AS avgrate FROM reviewhotel
    GROUP BY offering_id
    ''')
    avgrating.createOrReplaceTempView("avgrating")
    
    q1 = sqlc.sql('''
    SELECT DISTINCT reviewhotel.offering_id, avgrating.avgrate AS avg_rating, 
    reviewhotel.hotel_class, reviewhotel.locality, reviewhotel.name, reviewhotel.region_id FROM reviewhotel
    INNER JOIN avgrating
    ON avgrating.hotel_id=reviewhotel.offering_id
    ORDER BY avg_rating DESC, reviewhotel.offering_id ASC
    ''')
    
    q1.coalesce(1).write.option("delimiter","\t").csv("mingyuli_hw7_q1.csv",header=True)
    
    
    
    numrate = sqlc.sql('''
    SELECT locality, COUNT(review_id) as reviewnum, AVG(ratings_overall) as localrate FROM reviewhotel
    WHERE hotel_class>=4.0
    GROUP BY locality
    ''')
    numrate.createOrReplaceTempView("numrate")
    q2 = sqlc.sql('''
    SELECT DISTINCT reviewhotel.locality, numrate.reviewnum AS cnt, numrate.localrate AS avg_rating FROM numrate
    INNER JOIN reviewhotel
    ON reviewhotel.locality=numrate.locality
    ORDER BY cnt DESC
    ''')
    q2.coalesce(1).write.option("delimiter","\t").csv("mingyuli_hw7_q2.csv",header=True)
    
    
    hotellocalrate = sqlc.sql('''
    SELECT locality, hotel_class, AVG(ratings_overall) AS overallavg FROM reviewhotel
    WHERE num_helpful_votes>=2 AND author_num_helpful_votes>10
    GROUP BY locality, hotel_class
    ''')
    hotellocalrate.createOrReplaceTempView("hotellocalrate")
    avghelp = sqlc.sql('''
    SELECT locality, AVG(ratings_overall) AS locavg FROM reviewhotel
    WHERE num_helpful_votes>=2 AND author_num_helpful_votes>10
    GROUP BY locality
    ''')
    avghelp.createOrReplaceTempView("avghelp")
    q3=sqlc.sql('''
    SELECT hotellocalrate.locality, hotellocalrate.hotel_class, hotellocalrate.overallavg AS avg_rating FROM hotellocalrate
    LEFT OUTER JOIN avghelp
    ON avghelp.locality=hotellocalrate.locality
    ORDER BY avghelp.locavg DESC, hotellocalrate.hotel_class DESC
    ''')
    q3.coalesce(1).write.option("delimiter","\t").csv("mingyuli_hw7_q3.csv",header=True)
        
        