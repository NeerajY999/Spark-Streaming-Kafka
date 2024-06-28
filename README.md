# Spark Kafka Streaming Application

This application streams data from Kafka, parses the JSON data, and prints it to the console using Apache Spark.

## Requirements

- Python 3.6 or above
- Apache Spark 3.3.0
- PySpark
- Jupyter Notebook

## Installation

### Install Python and PySpark

1. Ensure you have Python 3.6 or above installed. You can download Python from the [official website](https://www.python.org/downloads/).

2. Install PySpark using pip:


### Apache Spark

1. Download and install Apache Spark 3.3.0 from the [official Apache Spark website](https://spark.apache.org/downloads.html).

## About the Application

This application performs the following tasks:

1. **Create the Spark Session**: Initializes a Spark session with the necessary configurations for streaming from Kafka.
2. **Create the Streaming DataFrame**: Reads streaming data from a specified Kafka topic.
3. **Parse the Kafka Data as JSON**: Converts the streamed data into JSON strings for further processing.
4. **Define the Schema**: Specifies the schema for the JSON data to structure and parse it accurately.
5. **Apply the Schema**: Parses the JSON strings into structured data using the defined schema.
6. **Start the Streaming Query**: Outputs the parsed data to the console in real-time.

## Running the Application

1. Save the provided code in a Jupyter Notebook file, e.g., `Spark_Streaming_Kafka.ipynb`.

2. Open the Jupyter Notebook:


3. Run the cells in the notebook to start the application.

## Note

- Ensure that the Kafka deployed URL is accessible from your environment.
- Adjust the schema and parsing logic according to your specific JSON data structure.
