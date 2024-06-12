from flask import Flask, jsonify, request
import sys
import os

sys.path.append(os.path.abspath(os.path.join("../Causal-Inference/scripts/")))
from data_cleaner import DataPipeline

app = Flask(__name__)

