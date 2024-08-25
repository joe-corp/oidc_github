from fastapi.testclient import TestClient
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

# Now you can import main
from main import app

client = TestClient(app)

def test_index():
    response = client.get("/")
    print(response)
    assert response.status_code == 200
    assert response.json() == "Hello World"