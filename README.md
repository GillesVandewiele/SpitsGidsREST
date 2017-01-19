# Spitsgids REST server

This is a REST server that uses machine learning to predict occupancy on the Belgian train network. An endpoint is currently running on `193.190.127.247`

## Example

GET http://localhost:8000/predict?departureTime=2017-01-15T17:00:55&vehicle=IC2816&from=008892007&to=008813003
```json
{
  "high_probability": "0.176085",
  "low_probability": "0.515583",
  "medium_probability": "0.308332",
  "prediction": "low"
}
```

## Running

0. Install the dependencies `(sudo) python setup.py install`
1. Start mongod on a certain port
2. Change the host and portnumber of your running MongoDB in `server.py` and then execute it: `python server.py`

## License

[here](LICENSE.txt)
