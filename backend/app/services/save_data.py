from sqlalchemy.orm import Session
from db.file_model import FileUpload, MappedData, FileProcessingLog

def save_file_analysis_to_db(
    file_name: str,
    file_type: str,
    matched_columns: list[str],
    unmatched_columns: list[str],
    total_records: int,
    status: str,
    mapped_data: list[dict],
    missing_columns: list[str],
    db: Session
):
    """
    Saves metadata and mapped content of a parsed file into the database.

    This function inserts a new file entry into the `FileUpload` table,
    stores the processing summary into the `FileProcessingLog` table,
    and saves the structured mapped data into the `MappedData` table.

    Args:
        file_name (str): The original name of the uploaded file.
        file_type (str): The type/extension of the file (e.g., csv, xlsx, pdf).
        matched_columns (list[str]): List of column names successfully mapped to schema fields.
        unmatched_columns (list[str]): List of column names that couldn't be matched.
        total_records (int): Total number of records parsed from the file.
        status (str): Final processing status (e.g., 'Accepted' or 'Rejected').
        mapped_data (list[dict]): List of dictionaries containing parsed and mapped row data.
        missing_columns (list[str]): List of schema fields missing from the uploaded data.
        db (Session): SQLAlchemy database session object.

    Returns:
        int: The database ID (`file_id`) of the inserted file record.

    Raises:
        Exception: Rolls back the transaction and re-raises any database error encountered.
    """
    try:
        file_record = FileUpload(file_name=file_name, file_type=file_type)
        db.add(file_record)
        db.flush() 

        metadata_record = FileProcessingLog(
            file_id=file_record.id,
            matched_columns=matched_columns,
            unmatched_columns=unmatched_columns,
            total_records=total_records,
            missing_columns=missing_columns,
            status=status,
        )
        db.add(metadata_record)

        mapped_data_record = MappedData(
            file_id=file_record.id,
            data=mapped_data
        )
        db.add(mapped_data_record)

        db.commit()
        db.refresh(file_record)
        return file_record.id

    except Exception as e:
        db.rollback()
        raise e 


def save_rejected_files(
    file_name: str,
    db: Session,
):
    """
    Saves metadata of a rejected file into the database.

    This function inserts a new file entry into the `FileUpload` table with a status of 'Rejected'.

    Args:
        file_name (str): The original name of the uploaded file.
        file_type (str): The type/extension of the file (e.g., csv, xlsx, pdf).
        status (str): Final processing status (should be 'Rejected').
        db (Session): SQLAlchemy database session object.

    Returns:
        int: The database ID (`file_id`) of the inserted file record.

    Raises:
        Exception: Rolls back the transaction and re-raises any database error encountered.
    """
    try:
        file_type = file_name.split(".")[-1] if "." in file_name else "unknown"
        file_name = file_name.split(".")[0]
        file_record = FileUpload(file_name=file_name, file_type=file_type)
        db.add(file_record)
        db.flush() 

        metadata_record = FileProcessingLog(
            file_id=file_record.id,
            status="rejected",
        )
        db.add(metadata_record)

        db.commit()
        db.refresh(file_record)
        return file_record.id

    except Exception as e:
        db.rollback()
        raise e