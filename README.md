from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder.appName("SearchTeaInXML").getOrCreate()

# Specify the S3 path containing XML files
s3_path = "s3://your_bucket/your_path/*.xml"

# Read XML files into a DataFrame
df = spark.read.format("xml").option("rowTag", "root").load(s3_path)

# Flatten the XML structure and convert to a single column of StringType
flat_df = df.selectExpr("explode_outer(to_json(struct(*))) as xml_content").select("xml_content").withColumn("xml_content", col("xml_content").cast(StringType()))

# Filter rows containing the word 'tea'
result_df = flat_df.filter(col("xml_content").contains("tea"))

# Show the results
result_df.show(truncate=False)

**User Story: Ratemaking Data Reload for 01/16 - Analysis**

**Brief Description:**
As a tech team member, I need to address the data refresh issue encountered by Ratemaking during their daily process execution on 01/16/2024. This issue resulted in the absence of quotes data in the AWS cloud, specifically from SIP jobs (Teradata to AWS 33) to the raw bucket. The impact extends to the data loaded for the same date in our rating process.

**Acceptance Criteria:**
1. Validate the fix implemented by the Ratemaking team to ensure proper processing of SIP jobs from Teradata to AWS 33.
2. Confirm the successful reload of quotes data to AWS cloud for the execution on 01/16/2024.
3. Verify that the data processed through SIP jobs is now reaching the raw bucket as expected.
4. Ensure that the data impacting the rating process for the date (2024-01-16) is correctly loaded and accessible.
5. Collaborate with Ratemaking team to understand the root cause and preventive measures for similar issues in the future.

**Assumptions:**
1. The Ratemaking team has implemented a fix to address the issue identified in their analysis.
2. Communication channels with the Ratemaking team are open for collaboration and updates.
3. Necessary permissions and access to relevant systems and data for validation are available.

**Dependencies:**
1. Availability of the Ratemaking team for collaboration and clarification on their fix.
2. Access to the production environment for validating the data reload and ensuring the impact on the rating process is resolved.
3. Timely communication of the fix status and any additional actions required from the Ratemaking team.
