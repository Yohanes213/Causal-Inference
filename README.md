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
