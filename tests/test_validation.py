import pytest
from datetime import date
from pydantic import ValidationError
from validation.schemas import WeatherRecord, WeatherBatch


# ─── Tests WeatherRecord ─────────────────────────────────────

class TestWeatherRecord:

    def valid_data(self):
        return {
            "city": "Paris",
            "latitude": 48.8566,
            "longitude": 2.3522,
            "date": date(2026, 4, 1),
            "temp_max": 18.5,
            "temp_min": 12.1,
            "precipitation_sum": 0.0,
        }

    def test_record_valide(self):
        record = WeatherRecord(**self.valid_data())
        assert record.city == "Paris"

    def test_temp_max_inferieur_temp_min(self):
        data = self.valid_data()
        data["temp_max"] = 5.0
        data["temp_min"] = 15.0
        with pytest.raises(ValidationError):
            WeatherRecord(**data)

    def test_temperature_aberrante(self):
        data = self.valid_data()
        data["temp_max"] = 999.0
        with pytest.raises(ValidationError):
            WeatherRecord(**data)

    def test_precipitation_negative(self):
        data = self.valid_data()
        data["precipitation_sum"] = -5.0
        with pytest.raises(ValidationError):
            WeatherRecord(**data)

    def test_ville_vide(self):
        data = self.valid_data()
        data["city"] = ""
        with pytest.raises(ValidationError):
            WeatherRecord(**data)

    def test_latitude_invalide(self):
        data = self.valid_data()
        data["latitude"] = 999.0
        with pytest.raises(ValidationError):
            WeatherRecord(**data)


# ─── Tests WeatherBatch ──────────────────────────────────────

class TestWeatherBatch:

    def valid_records(self):
        return [
            {
                "city": "Paris", "latitude": 48.8566, "longitude": 2.3522,
                "date": date(2026, 4, 1), "temp_max": 18.5, "temp_min": 12.1,
                "precipitation_sum": 0.0,
            },
            {
                "city": "Lyon", "latitude": 45.764, "longitude": 4.8357,
                "date": date(2026, 4, 1), "temp_max": 17.4, "temp_min": 10.2,
                "precipitation_sum": 2.5,
            },
        ]

    def test_batch_valide(self):
        batch = WeatherBatch(records=self.valid_records())
        assert len(batch.records) == 2

    def test_batch_vide(self):
        with pytest.raises(ValidationError):
            WeatherBatch(records=[])

    def test_doublons_detectes(self):
        records = self.valid_records()
        records.append(records[0])  # doublon Paris / même date
        with pytest.raises(ValidationError):
            WeatherBatch(records=records)