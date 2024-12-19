import re
import json


log_data = """
04/12/2024 10:58:28 [Alteração de Status GP - Gerado Pedido]
04/12/2024 11:20:12 [Atualizar ID do Pedido na API]  [Resposta da API = {"data":{"createGroupedOrder":{"id":24305126,"grouped_order_code":24305126,"client_identification":"84598929000154","wholesaler":"84521053000300","client_code":"6204","commercial_condition":"CSM","status":"AWAITING_PROCESSING","total_products":1}}}]
04/12/2024 11:20:12[Json do Pedido enviado para a API]
mutation createGroupedOrder{ createGroupedOrder(client_identification:"84598929000154" wholesaler:"84521053000300" client_code:"6204" commercial_condition:"CSM" products: [{ean:"7891058020286" ordered_quantity:2 wholesaler_discount:0.00}]) {id grouped_order_code client_identification wholesaler client_code commercial_condition status total_products} }
"""


id_pattern = r'"id":(\d+)'
response_pattern = r'\[Resposta da API = (.+?)\]'
ean_pattern = r'ean:"(\d+)"'
quantity_pattern = r'ordered_quantity:(\d+)'
discount_pattern = r'wholesaler_discount:(\d+\.\d{2})'


id_match = re.search(id_pattern, log_data)
order_id = id_match.group(1) if id_match else None


response_match = re.search(response_pattern, log_data)
response_data = json.loads(response_match.group(1)) if response_match else None

ean_match = re.search(ean_pattern, log_data)
quantity_match = re.search(quantity_pattern, log_data)
discount_match = re.search(discount_pattern, log_data)

ean = ean_match.group(1) if ean_match else None
quantity = quantity_match.group(1) if quantity_match else None
discount = discount_match.group(1) if discount_match else None


with open('extraction_log.txt', 'a') as log_file:
    log_file.write(f"ID: {order_id}\n")
    log_file.write(f"EAN: {ean}\n")
    log_file.write(f"Response Quantity: {quantity}\n")
    log_file.write(f"Discount Percentage: {discount}\n")
    log_file.write("\n")  

print(f"ID: {order_id}")
print(f"EAN: {ean}")
print(f"Response Quantity: {quantity}")
print(f"Discount Percentage: {discount}")
