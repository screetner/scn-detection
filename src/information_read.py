import json
import os
from src.custom_types.session_information import SessionInformation



def read_session_information() -> SessionInformation:
    file_path = os.path.join(os.path.dirname(__file__), '../downloaded/information.json')
    try:
        with open(file_path, 'r') as session_information_file:
            data = json.load(session_information_file)
            return data
    except FileNotFoundError:
        raise FileNotFoundError(f"ไม่พบไฟล์ที่: {file_path}")
    except json.JSONDecodeError:
        raise ValueError(f"ไฟล์ {file_path} มีรูปแบบ JSON ไม่ถูกต้อง")