import holidays
import json

# Festivos para España, Comunidad de Madrid
es_holidays = holidays.Spain(prov="MD", years=[2024, 2025])

# Convertir a lista de diccionarios
holiday_list = [{"date": str(date), "name": name} for date, name in es_holidays.items()]

# Imprimir JSON
print(json.dumps(holiday_list, indent=2))
