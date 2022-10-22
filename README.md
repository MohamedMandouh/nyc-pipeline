Project for [Data Engineering ZoomCamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)

<image src= "https://user-images.githubusercontent.com/41024902/197347200-7d2e46b2-644f-4f8f-bc7a-05a70faf5519.png" width =20 height= 20> Terraform   
  Step 1: Create the required GCP infrastructure using Terraform.  

<image src="https://user-images.githubusercontent.com/41024902/197346916-bc8482f4-8312-4838-9513-073bc8a51a6e.png" width =20 height= 20> Airflow  
   Step 2: Download archive data of nyc-yellow-taxi trip data.  
   Step 3: Load the raw files into a GCP bucket. 🪣  
   Step 4: Clean the raw data and enrich it with zones lookup-table using ⚡ PySpark.  
   Step 5: Write the result into a GCP bucket.  
   Step 6: Load the result files into a Bigquery table
  
<image src= "https://github.com/buildkite/emojis/blob/main/img-buildkite-64/kafka.png" width =20 height= 20> Kafka  
  Step 7: Produce a stream of new trip data (1-minute mini-batch)  
  Step 8: Join the stream with zones lookup-table using Pandas. 🐼  
  Step 9: Write the result into Bigquery using the streaming API.  
  
  🔍 Bigquery  
    Step 10: Create a view for summarizing trip data.  
    Step 11: Produce a DataStudio report for the view.  
