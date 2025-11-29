# Imports
from quixstreams import Application
from dotenv import load_dotenv
import logging
import duckdb
import time
import json
import os
import sys

# os.getenv() will now refer to the .env file in the directory
load_dotenv()

# Kafka address
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")

# Key names for State Vector Response
state_vector_keys = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity", 
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk", 
    "spi", "position_source"
]

# Function to run consumer
def main():
    
    logger.info("Initializing Consumer to Read airspace-events")

    # Setting up application
    app = Application(
        broker_address = KAFKA_BROKER,
        consumer_group = 'airspace-consumer',
        auto_offset_reset = 'earliest'
    )

    # Initializing Kafka topic to consume from
    topic = app.topic(
        name = 'airspace-events',
        value_deserializer = 'bytes'
    )

    # Initializing consumer and subscribing to airspace-events topic
    consumer = app.get_consumer()
    consumer.subscribe([topic.name])

    # Connecting to duckdb
    try:
        con = duckdb.connect(database = 'airspace-events.duckdb', read_only = False)
        logger.info("Connect to DuckDB Instance")
    except Exception as e:
        logger.error(f"Failed to Connect to DuckDB: {e}")
        raise SystemExit(1)

    # Creating table in duckdb if it does not already exists
    try:
        con.execute(f"""
            -- Comment
            CREATE TABLE IF NOT EXISTS airspace (
                icao24 VARCHAR(6) NOT NULL,
                callsign VARCHAR(10) NULL,
                origin_country VARCHAR(50) NULL,
                time_position INTEGER NULL,
                last_contact INTEGER NOT NULL,
                longitude DOUBLE PRECISION NULL,
                latitude DOUBLE PRECISION NULL,
                baro_altitude DOUBLE PRECISION NULL,
                on_ground BOOLEAN NOT NULL,
                velocity DOUBLE PRECISION NULL,
                true_track DOUBLE PRECISION NULL,
                vertical_rate DOUBLE PRECISION NULL,
                sensors INTEGER ARRAY NULL,
                geo_altitude DOUBLE PRECISION NULL,
                squawk VARCHAR(4) NULL,
                spi BOOLEAN NOT NULL,
                position_source INTEGER NULL,
                PRIMARY KEY (icao24, last_contact));        
            """)
        logger.info("Created DuckDB table if it did not already exist")
    except Exception as e:
        logger.error(f"Failed to create airspace table in DuckDB: {e}")
        raise SystemExit(1)

    # Query for inserting consumed state vectors into Duckdb
    column_names = ', '.join(state_vector_keys)
    placeholder_values = ', '.join(['?'] * len(state_vector_keys))
    insert_query = f"INSERT OR REPLACE INTO airspace ({column_names}) VALUES ({placeholder_values})"

    # List for batch insertion
    events_to_insert = []
    messages_processed = 0

    # Inserting events into duckdb
    try:
        while True:
            # Consume from topic
            message = consumer.poll(timeout = 1)

            # Insert to duckdb if their is a message, otherwise sleep for 5 seconds
            if message is not None and not message.error():
                # Extract state vector
                value = message.value()
                state_vector = json.loads(value.decode('utf-8'))

                try:
                    # Create tuple with key-value pairs
                    data = json.loads(value.decode('utf-8'))
                    row_list = [data.get(key) for key in state_vector_keys]
                    row_tuple = tuple(row_list)

                    # Append event to the list for batch insertion
                    events_to_insert.append(row_tuple)

                    # Insert events when batch size reaches 2000
                    if len(events_to_insert) >= 2000:
                        con.executemany(insert_query, events_to_insert)
                        con.commit()

                        messages_processed += len(events_to_insert)
                        logger.info(f"Batch committed. Total records inserted: {messages_processed}")
                        
                        # Empty batch list
                        events_to_insert = []
                except Exception as e:
                    logger.error(f"Failed to insert message {messages_processed+1} into DuckDB")
            elif message is None:
                logger.info("Waiting for more messages")
                time.sleep(5)
    except KeyboardInterrupt:
        logger.warning("Keyboard Interruption")
    finally:
        # Ensure message queued for batch insertion are handled when script is stopped
        if len(events_to_insert) > 0 and con:
            try:
                con.executemany(insert_query, events_to_insert)
                con.commit()
                messages_processed += len(events_to_insert)
                logger.info(f"Final cleanup commit successful. Total records inserted: {messages_processed}")
            except Exception as e:
                logger.error(f"Failed to commit remaining batch on shutdown: {e}")
        # Close duckdb connection when program ends
        if con:
            con.close()
            logger.info("Closed DuckDB connection")

        consumer.close()
        logger.info("Consumer Stopped")

# Initialize logger and run main function
if __name__ == '__main__':
    try:
        logging.basicConfig(
            level = logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='consumer.log',
            filemode='a'
        )
        logger = logging.getLogger(__name__) # Get a logger instance
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(console)
        main()
    except KeyboardInterrupt:
        pass