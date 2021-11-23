'''
Created on Jun 28, 2020

@author: oirraza
'''

import json
import jsonschema


if __name__ == '__main__':
    
    
    schema3 = {
        "type": "object",
        "properties": {
            "fecha": {"type": "boolean"},
            "hora": {"type": "boolean"},
            "instalacion": {"type": "boolean"},
            "medidor": {"type": "boolean"},
            "temperatura": {"type": "boolean"},
            "presion": {"type": "boolean"},
            "caudal_instantaneo_gross": {"type": "boolean"},
            "acumulador_gross_no_reseteable": {"type": "boolean"},
            "acumulador_pulsos_brutos_no_reseteable": {"type": "boolean"},
            "factor_k_del_medidor": {"type": "boolean"},
            "altura_liquida": {"type": "boolean"},
            "acumulador_masa_no_reseteable": {"type": "boolean"},
            "volumen_acumulado_24_hs": {"type": "boolean"},
            "volumen_acumulado_hoy": {"type": "boolean"},
            "sh2": {"type": "boolean"},
            "n2": {"type": "boolean"},
            "c6_mas": {"type": "boolean"},
            "nc5": {"type": "boolean"},
            "densidad_relativa": {"type": "boolean"},
            "co2": {"type": "boolean"},
            "caudal_instantaneo_a_9300": {"type": "boolean"}, 
            "c1": {"type": "boolean"}, 
            "c2": {"type": "boolean"},
            "c3": {"type": "boolean"},
            "ic4": {"type": "boolean"},
            "nc4": {"type": "boolean"},
            "ic5": {"type": "boolean"},
            "poder_calorifico": {"type": "boolean"}
        },
        "required": ["fecha", "hora", "instalacion", "medidor"],
        "additionalProperties": False
    }
    
    validJsonData3 = """{
        "fecha": true,
        "hora": true,
        "instalacion": true,
        "medidor": true,
        "temperatura": true,
        "presion": true,
        "caudal_instantaneo_gross": true,
        "acumulador_gross_no_reseteable": false,
        "acumulador_pulsos_brutos_no_reseteable": true,
        "factor_k_del_medidor": true
    }"""
    

    jsonData3 = json.loads(validJsonData3)
    print(jsonData3)
    print(f"jsonData3: {jsonschema.validate(instance=jsonData3, schema=schema3)}")
