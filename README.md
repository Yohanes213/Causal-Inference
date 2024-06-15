# Gokada Delivery Optimization Project

### Overview
Gokada, the largest last-mile delivery service in Nigeria, has partnered with motorbike owners and drivers to deliver parcels across Lagos. With over a million deliveries completed in less than a year and a fleet of over 1200 riders, Gokada faces the challenge of optimizing the placement of pilots (drivers) to meet the high demand effectively. This project aims to understand the primary causes of unfulfilled delivery requests and recommend driver locations that increase the fraction of completed orders, thereby enhancing client satisfaction and business growth.

### Data


| Column | Non-Null Count | Dtype |
|-----------------|-----------------|-----------------|
| Trip ID  | 536020  | int64  |
| Trip Origin  | 536020  | address  |
| Trip Destination  | 536020  | address  |
| Trip Start Time   | 536020 | timestamp |
| Trip End Time   | 536020 | timestamp |



| Column | Non-Null Count | Dtype |
|-----------------|-----------------|-----------------|
| id  | 1557740  | int64  |
| order_id  | 1557740  | int64  |
| driver_id  | 1557740  | int64  |
| driver_action  | 1557740  | object  |
| lat   | 1557740 | float64 |
| lng   | 1557740 | float64 |
| created_at  | 0  | float64  |
| updated_at  | 0  | float64  |


### Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/yourusername/Causal-Inference.git
   cd Causal-Inference
   ```
2. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```

### Usage

##### Data Cleaning and Processing
  - Run the data cleaning script to preprocess and clean the data:
  ```sh
  python scripts/data_cleaner.py
  ```
#### Web Application
Open the index.html file in a web browser. Enter the order_id to visualize the respective driver's location on a map.


![1Screenshot from 2024-06-15 10-29-25](https://github.com/Yohanes213/Causal-Inference/assets/99422479/a266a5ce-7227-4399-aecd-f0772d64ed20)


### Contributions

Contributions to this project are welcome.

### License

This project is licensed under the MIT License.
