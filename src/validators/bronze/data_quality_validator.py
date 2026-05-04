import logging

logger = logging.getLogger(__name__)

DQ_EMPTY_THRESHOLD = 0.5  # 50% empty threshold

def validate_bronze_data(
    data: list, 
    source_name: str, 
    critical_fields: tuple = ("title", "company"),
    threshold: float = DQ_EMPTY_THRESHOLD
) -> None:
    """
    Basic data quality check for Bronze layer.
    Checks if critical fields are empty for more than a certain threshold.
    """
    if not data:
        return

    total = len(data)
    invalid_count = 0
    
    for item in data:
        is_valid = True
        for field in critical_fields:
            if not item.get(field):
                is_valid = False
                break
        if not is_valid:
            invalid_count += 1
            
    ratio = invalid_count / total
    
    # Chúng ta trả về metric để ghi báo cáo trước khi raise lỗi
    if ratio > threshold:
        msg = f"Data quality check failed for {source_name}: {invalid_count}/{total} records ({ratio:.1%}) are missing critical fields."
        logger.error(msg)
        return invalid_count, total, msg
    
    if invalid_count > 0:
        msg = f"Data quality warning for {source_name}: {invalid_count}/{total} records ({ratio:.1%}) missing critical fields."
        logger.warning(msg)
        return invalid_count, total, msg

    return invalid_count, total, ""
