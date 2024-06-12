from flask import Flask, jsonify, request
from flask_cors import CORS
import sys
import os

sys.path.append(os.path.abspath(os.path.join("../Causal-Inference/scripts/")))
from data_cleaner import DataPipeline

app = Flask(__name__)
CORS(app)  # This will enable CORS for all routes

pipeline = DataPipeline('Nigeria')

delivery_requests = pipeline.read_data('data/driver_locations_during_request.csv')
completed_orders = pipeline.read_data('data/nb.csv')

@app.route('/order/<int:order_id>')
def get_order_details(order_id):
    order_details = completed_orders[completed_orders['Trip ID'] == order_id]
    if not order_details.empty:
        order_details[['origin_lat', 'origin_lon']] = order_details['Trip Origin'].str.split(',', expand=True).astype(float)
        order_details[['dest_lat', 'dest_lon']] = order_details['Trip Destination'].str.split(',', expand=True).astype(float)
        order_details = order_details.to_dict('records')
        return jsonify(order_details)
    return jsonify({'error': 'Order not found'}), 404

@app.route('/driver/<int:order_id>')
def get_driver_movements(order_id):
    driver_details = delivery_requests[delivery_requests['order_id'] == order_id]
    if not driver_details.empty:
        accepted_driver = driver_details[driver_details['driver_action'] == 'accepted'][['lat', 'lng']].values.tolist()
        rejected_drivers = driver_details[driver_details['driver_action'] == 'rejected'][['lat', 'lng']].values.tolist()
        response = {
            'accepted_driver': accepted_driver,
            'rejected_drivers': rejected_drivers
        }
        return jsonify(response)
    return jsonify({'error': 'Order not found'}), 404

@app.route('/unfulfilled_requests')
def get_unfulfilled_requests():
    unfulfilled_requests = delivery_requests[delivery_requests['driver_action'] != 'accepted']
    return jsonify(unfulfilled_requests.to_dict('records'))

if __name__ == '__main__':
    app.run(debug=True)
