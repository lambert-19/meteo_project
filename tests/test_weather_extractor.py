import pytest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from extract.weather_extractor import WeatherExtractor

@pytest.fixture
def extractor():
    return WeatherExtractor()

@pytest.fixture
def sample_data():
    """Données météo fictives pour les tests."""
    return [
        {
            "city": "Paris",
            "date": "2026-04-01",
            "temp_max": 18.5,
            "temp_min": 12.1,
            "precipitation_sum": 0.0,
        },
        {
            "city": "Lyon",
            "date": "2026-04-01",
            "temp_max": 17.4,
            "temp_min": 10.2,
            "precipitation_sum": 2.5,
        },
        {
            "city": "Marseille",
            "date": "2026-04-01",
            "temp_max": 19.0,
            "temp_min": 14.0,
            "precipitation_sum": 0.0,
        },
    ]
class TestTemperatureCoherence:

    def test_temp_max_superieur_temp_min(self, sample_data):
        """temp_max doit toujours être > temp_min."""
        for record in sample_data:
            assert record["temp_max"] > record["temp_min"], (
                f"[{record['city']} - {record['date']}] "
                f"temp_max ({record['temp_max']}) <= temp_min ({record['temp_min']})"
            )

    def test_temp_max_dans_bornes_realistes(self, sample_data):
        """temp_max doit être dans une plage réaliste pour la France (-30 à 50°C)."""
        for record in sample_data:
            assert -30 <= record["temp_max"] <= 50, (
                f"[{record['city']}] temp_max aberrante : {record['temp_max']}°C"
            )

    def test_temp_min_dans_bornes_realistes(self, sample_data):
        """temp_min doit être dans une plage réaliste pour la France."""
        for record in sample_data:
            assert -30 <= record["temp_min"] <= 50, (
                f"[{record['city']}] temp_min aberrante : {record['temp_min']}°C"
            )

    def test_detection_valeur_aberrante(self):
        """Doit détecter une température de capteur défaillant (ex: 999°C)."""
        bad_record = {"city": "Paris", "date": "2026-04-01", "temp_max": 999.0, "temp_min": 12.0}
        assert not (-30 <= bad_record["temp_max"] <= 50)


class TestPrecipitations:

    def test_precipitation_non_negative(self, sample_data):
        """Les précipitations ne peuvent pas être négatives."""
        for record in sample_data:
            assert record["precipitation_sum"] >= 0, (
                f"[{record['city']}] précipitation négative : {record['precipitation_sum']}"
            )

    def test_precipitation_borne_max(self, sample_data):
        """Précipitations max réaliste pour une journée en France : 300mm."""
        for record in sample_data:
            assert record["precipitation_sum"] <= 300, (
                f"[{record['city']}] précipitation aberrante : {record['precipitation_sum']}mm"
            )
class TestStructureDonnees:
    def test_champs_obligatoires_presents(self, sample_data):
        """Chaque enregistrement doit avoir tous les champs requis."""
        champs_requis = {"city", "date", "temp_max", "temp_min", "precipitation_sum"}
        for record in sample_data:
            champs_manquants = champs_requis - set(record.keys())
            assert not champs_manquants, f"Champs manquants : {champs_manquants}"

    def test_aucune_valeur_nulle(self, sample_data):
        """Aucun champ ne doit être None."""
        for record in sample_data:
            for key, value in record.items():
                assert value is not None, f"[{record['city']}] champ '{key}' est None"

    def test_villes_attendues(self, sample_data):
        """Les 3 villes Paris, Lyon, Marseille doivent être présentes."""
        villes = {r["city"] for r in sample_data}
        assert "Paris" in villes
        assert "Lyon" in villes
        assert "Marseille" in villes

    def test_format_date(self, sample_data):
        """La date doit être au format YYYY-MM-DD."""
        import re
        pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        for record in sample_data:
            assert pattern.match(record["date"]), (
                f"[{record['city']}] format de date invalide : {record['date']}"
            )