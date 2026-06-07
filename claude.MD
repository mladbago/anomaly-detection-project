Act as an expert data engineer and backend developer. We need to upgrade our payment card anomaly detection system to include a real-time Streamlit visualization dashboard, establish a robust testing strategy, and compile everything into a comprehensive README.md file.

### Original Project Specification (Translated)
"The objective of the project is to develop software for detecting anomalies in payment card transactions.
1. The architecture of the solution must be designed.
2. The software design should contain a description of the individual components.
3. An anomaly simulator must be developed.
4. The software should include the following components:
   a. Standard Kafka, Flink, and MongoDB software.
   b. A payment card transaction simulator, which will simultaneously act as a Kafka message producer.
   c. A test Kafka consumer that allows for the analysis of the correctness of generated data along with data visualization.
   d. An anomaly detector (Flink application) that will read data from Kafka, detect anomalies, and send alert information to a dedicated Kafka topic.
   e. A program for reading alerts, visualization, and notifying about alerts.
5. Assumptions for the payment card simulator:
   a. Transactions will be generated for 10,000 different cards.
   b. Generated data should contain: Card ID, User ID, Transaction location (GPS coordinates), Transaction value, Available spending limit on the card.
   c. Data should be generated in JSON format.
   d. The generator must allow for generating various types of anomalies, e.g., sudden change in transaction value, sudden change in location, transaction frequency, etc.
6. Assumptions for the anomaly detector:
   a. Data should be read from Kafka in near real-time.
   b. The anomaly detection method(s) should rely on statistical-based algorithms.
   c. Temporary memory should be provided, e.g., for storing frequent locations."

### Crucial Technical Updates & Requirements:
1. STREAMLIT VISUALIZATION DASHBOARD: Replace the terminal-based 'test-consumer' and 'alert-dashboard' scripts or supplement them with a fully featured Python Streamlit application containerized in Docker. The dashboard must:
   - Connect to MongoDB and Kafka to show live incoming transactions.
   - Display a real-time metric counter of total anomalies caught vs. normal transactions.
   - Display a live map (using Streamlit's native map functions) showing where the transactions or 'gps_jump' anomalies are occurring.
   - Display live charts (e.g., transaction volume over time, anomaly breakdown by type: amount_spike, gps_jump, rapid_burst).
2. NO LOCAL JAR FILES: Ensure the `flink-job/Dockerfile` automatically pulls down all necessary Apache Flink Kafka connectors using `wget` or `curl` directly into Flink's library directory during the Docker build stage. No local jars in the repository.
3. ONE-COMMAND SETUP: Ensure the updated `docker-compose.yml` launches everything seamlessly via `docker compose up --build -d`—including exposing the new Streamlit interface on an accessible port (e.g., 8501).
4. NO PROJECT REPORT: Do not generate a formal academic text report. Focus entirely on code execution, verification, and documentation inside the README.

### Testing & Verification Requirements:
I need to prove and ensure that the pipeline is processing data *correctly* and that the statistical algorithms are actually accurate. Implement and document a clear data testing strategy:
- Validation Test: How do we verify that the simulator's JSON schema perfectly matches what Flink expects?
- Precision Test: How do we prove that the Flink streaming engine isn't missing anomalies? (Tip: The simulator sets `is_injected_anomaly: true`. Flink should flag exactly those, allowing us to evaluate the system's accuracy).
- State Test: How do we ensure Flink's temporary memory state resets or drops data correctly after windows close?

### Documentation Task: Update `README.md`
Please overwrite or generate a comprehensive `README.md` file in the root directory from scratch. It must contain:
- A text-based/ASCII diagram mapping out the entire system architecture (including the new Streamlit Dashboard and database interactions).
- Step-by-step instructions on how to run, monitor, and test the project completely from the very beginning.
- A dedicated **"Testing Guide"** explaining exactly how a user can verify system correctness, query MongoDB to validate metrics, and ensure that Flink's alerts perfectly align with the simulator's injected anomalies.
- Verification commands for checking the health of Kafka, Flink, and MongoDB.
- An explanation of the statistical rules implemented (Rolling Z-Score, Haversine speed limits, sliding window burst counts).

Please inspect the directory structure, build the Streamlit application, modify the configurations, and output the code along with the updated README.md file.