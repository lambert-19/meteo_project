from pydantic import BaseModel, field_validator, model_validator
from typing import Optional
from datetime import date


class WeatherRecord(BaseModel):
    """Schéma de validation d'un enregistrement météo venant de l'API."""

    city: str
    latitude: float
    longitude: float
    date: date
    temp_max: float
    temp_min: float
    precipitation_sum: float
    region: Optional[str] = None
    elevation: Optional[float] = None

    # ─── Validations individuelles ───────────────────────────

    @field_validator("city")
    @classmethod
    def city_non_vide(cls, v):
        if not v or not v.strip():
            raise ValueError("Le nom de la ville ne peut pas être vide.")
        return v.strip()

    @field_validator("temp_max", "temp_min")
    @classmethod
    def temperature_dans_bornes(cls, v):
        if not (-30 <= v <= 60):
            raise ValueError(f"Température aberrante détectée : {v}°C (hors bornes -30/60)")
        return v

    @field_validator("precipitation_sum")
    @classmethod
    def precipitation_non_negative(cls, v):
        if v < 0:
            raise ValueError(f"Précipitation négative impossible : {v}mm")
        if v > 300:
            raise ValueError(f"Précipitation journalière aberrante : {v}mm")
        return v

    @field_validator("latitude")
    @classmethod
    def latitude_valide(cls, v):
        if not (-90 <= v <= 90):
            raise ValueError(f"Latitude invalide : {v}")
        return v

    @field_validator("longitude")
    @classmethod
    def longitude_valide(cls, v):
        if not (-180 <= v <= 180):
            raise ValueError(f"Longitude invalide : {v}")
        return v

    # ─── Validation croisée (temp_max > temp_min) ────────────

    @model_validator(mode="after")
    def temp_max_superieur_temp_min(self):
        if self.temp_max <= self.temp_min:
            raise ValueError(
                f"temp_max ({self.temp_max}) doit être supérieure à temp_min ({self.temp_min})"
            )
        return self


class WeatherBatch(BaseModel):
    """Schéma de validation d'un batch complet (plusieurs enregistrements)."""

    records: list[WeatherRecord]

    @model_validator(mode="after")
    def batch_non_vide(self):
        if not self.records:
            raise ValueError("Le batch est vide — aucune donnée reçue de l'API.")
        return self

    @model_validator(mode="after")
    def pas_de_doublons(self):
        paires = [(r.city, str(r.date)) for r in self.records]
        if len(paires) != len(set(paires)):
            raise ValueError("Doublons détectés dans le batch (city + date identiques).")
        return self

    def valid_records(self) -> list[WeatherRecord]:
        """Retourne uniquement les enregistrements valides."""
        return self.records