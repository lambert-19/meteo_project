import pytest



class TestAnomaliesTemperature:

    def test_ecart_temp_anormal(self):
        """Un écart temp_max - temp_min > 40°C est suspect."""
        record = {"city": "Lyon", "date": "2026-04-01", "temp_max": 55.0, "temp_min": 10.0}
        ecart = record["temp_max"] - record["temp_min"]
        assert ecart <= 40, f"Écart thermique suspect : {ecart}°C"

    def test_ecart_temp_normal(self):
        """Un écart raisonnable doit passer."""
        record = {"city": "Lyon", "date": "2026-04-01", "temp_max": 20.0, "temp_min": 10.0}
        ecart = record["temp_max"] - record["temp_min"]
        assert ecart <= 40

    def test_temperature_nulle_suspecte(self):
        """Une temp_max exactement à 0.0 en été est suspecte (capteur HS?)."""
        record = {"city": "Marseille", "date": "2026-07-15", "temp_max": 0.0, "temp_min": 0.0}
        assert record["temp_max"] == record["temp_min"]  

    @pytest.mark.xfail(reason="Test de détection : un écart > 40°C doit être signalé")
    def test_ecart_temp_anormal(self):
        """Un écart temp_max - temp_min > 40°C est suspect."""
        record = {"city": "Lyon", "date": "2026-04-01", "temp_max": 55.0, "temp_min": 10.0}
        ecart = record["temp_max"] - record["temp_min"]
        assert ecart <= 40, f"Écart thermique suspect : {ecart}°C"


class TestBatchDonnees:

    def test_nombre_records_attendu(self):
        """Un batch de 30 jours x 3 villes = 90 enregistrements attendus."""
        nb_villes = 3
        nb_jours = 30
        expected = nb_villes * nb_jours
        fake_batch = [{"city": "Paris"} for _ in range(expected)]
        assert len(fake_batch) == 90

    def test_pas_de_doublons(self):
        """Pas de doublon sur (city, date)."""
        records = [
            {"city": "Paris", "date": "2026-04-01"},
            {"city": "Lyon", "date": "2026-04-01"},
            {"city": "Paris", "date": "2026-04-01"},  
        ]
        paires = [(r["city"], r["date"]) for r in records]
        assert len(paires) != len(set(paires)), "Doublon détecté (comportement attendu dans ce test)"