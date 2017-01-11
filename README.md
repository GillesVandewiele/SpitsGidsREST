# Spitsgids REST server

This is a REST server that uses machine learning to predict occupancy on the Belgian train network.

## Endpoints

### GET `/predict_by_vehicle`
#### Parameters

* `departureTime`
* `vehicle`

#### Returns

Returns a predicted occupancy. Terms can be found at https://api.irail.be/terms/

```json
{
  "prediction": "low"
}
```

### GET `/predict_by_from_to`
#### Parameters

* `departureTime`
* `from`
* `to`

#### Returns

Returns a predicted occupancy. Terms can be found at https://api.irail.be/terms/

```json
{
  "prediction": "low"
}
```

### GET `/predict`
#### Parameters

* `departureTime`
* `vehicle`
* `from`
* `to`

#### Returns

Returns a predicted occupancy. Terms can be found at https://api.irail.be/terms/

```json
{
  "prediction": "low"
}
```

## Running

0. Install the dependencies `...`
1. First make the prediction model by running `python xgb.py`
2. Start the server with `python server.py`

## License

...
