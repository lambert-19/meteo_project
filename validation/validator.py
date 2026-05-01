from pydantic import ValidationError
from validation.schemas import WeatherRecord, WeatherBatch
import logging

logger = logging.getLogger(__name__)


def validate_record(record: dict) -> WeatherRecord | None:
    """
    Valide un enregistrement météo unique.
    Retourne l'objet validé ou None si invalide.
    """
    try:
        return WeatherRecord(**record)
    except ValidationError as e:
        logger.warning(f"❌ Enregistrement invalide ignoré : {e}")
        return None


def validate_batch(records: list[dict]) -> list[WeatherRecord]:
    """
    Valide une liste d'enregistrements.
    Retourne uniquement les enregistrements valides.
    """
    valid = []
    invalid_count = 0

    for record in records:
        result = validate_record(record)
        if result:
            valid.append(result)
        else:
            invalid_count += 1

    logger.info(f"✅ {len(valid)} enregistrements valides / ❌ {invalid_count} rejetés")
    return valid