# STEDI-Human-Balance-Analytics

**Project Details**

STEDI Team is working hard to develop a hardware STEDI Step Trainer to

1. Trains the user to do STEDI balance exercise has sensors on the device that collect data to train a machine learning algorih to detect steps has a companion mobile app that collects customer data and interacts with device

2. Many customers has already received the step trainers and installed the mobile applice and using then, sensor records the distance. App alsi uses phone accelerometer to detect directions

3. some early adopters aggree to share their data for research purposes

**Data**
Customer Records
Step Trainer Records
Accelerometer Records

**Implementation**


**Landing Zone**
 I stored the customer, accelerometer and step trainer raw data in AWS S3 bucket.


**The AWS glue data catalog,**
 I created a glue tables so that I can query the data using AWS athena.


**Trusted Zone**
I created AWS Glue jobs to make transofrmations on the raw data in the landing zones.

**Glue job scripts**

1. customer_landing_to_trusted.py - This script transfers customer data from the 'landing' to 'trusted' zones. It filters for customers who have agreed to share data with researchers.

2. Accelerometer Landing to Trusted.py - This script transfers accelerometer data from the 'landing' to 'trusted' zones. Using a join on customer_trusted and accelerometer_landing, It filters for Accelerometer readings from customers who have agreed to share data with researchers.

3. Step Trainer Landing to Trusted.py - This script transfers Step Trainer data from the 'landing' to 'trusted' zones. Using a join on customer_curated and step_trainer_landing, It filters for customers who have accelerometer data and have agreed to share their data for research with Step Trainer readings.


4. Customer Trusted to Curated.py - This script transfers customer data from the 'trusted' to 'curated' zones. Using a join on customer_trusted and accelerometer_landing, It filters for customers with Accelerometer readings and have agreed to share data with researchers.

5. machine_learning_curated.py: This script is used to build aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data.

I had also had some issues while creating Step Trainer Landing to Trusted.py and machine_learning_curated.py couldn't run the script, but I am hoping the scripts are correct.
