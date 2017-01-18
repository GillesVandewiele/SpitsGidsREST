# Spitsgids REST server

This is a REST server that uses machine learning to predict occupancy on the Belgian train network.

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

0. Install the dependencies `...`
1. Start the server with `python server.py`

## License

[here](LICENSE.txt)
