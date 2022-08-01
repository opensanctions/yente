from typing import Any, Dict


def find_result_id(response: Dict[str, Any], id: str) -> bool:
    for res in response['results']:
        if res['id'] == id:
            return True
    return False

def first_result_id(response: Dict[str, Any], id: str) -> bool:
    for res in response['results']:
        if res['id'] == id:
            return True
        break
    return False